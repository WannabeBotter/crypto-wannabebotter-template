import os
import time
import math
import pdb
import gc
import argparse
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import numba
from matplotlib import figure
import matplotlib.pyplot as plt
import talib
import mlflow
from tqdm import tqdm
from memory_profiler import profile

import cvxpy as cp
from pypfopt.expected_returns import mean_historical_return, returns_from_prices
from pypfopt.risk_models import CovarianceShrinkage
from pypfopt.efficient_frontier import EfficientFrontier, EfficientSemivariance
from pypfopt import objective_functions
from pypfopt import plotting
import optuna

# ログ系のインポート
from logging import Logger, getLogger, basicConfig, Formatter
import logging
from rich.logging import RichHandler

# 時間足データを保存する時に使うTimescaleDB用のユーティリティライブラリの設定
from crypto_bot_config import pg_config, binance_testnet_config, binance_config, pybotters_apis
from async_manager import AsyncManager
from timescaledb_manager import TimescaleDBManager
from exchange_manager import ExchangeManager
from pybotters_manager import PyBottersManager
from timebar_manager import TimebarManager

def calc_cycle_constants(params):
    ROWS_REBALANCE = int(params['rebalance_interval_hour'] * 60 * 60 / params['timebar_interval_sec'])
    ROWS_EXECUTION = int(params['rebalance_time_sec'] / params['timebar_interval_sec'])
    ROWS_WAIT_FOR_EXECUTION = int(params['rebalance_wait_sec'] / params['timebar_interval_sec'])
    ROWS_WEIGHT_CALC = int(params['weight_calc_period'] * 24 * 60 * 60 / params['timebar_interval_sec'])
    ROWS_COMPONENTS_SWAP_INTERVAL = int(params['components_swap_interval'] * ROWS_REBALANCE)
    ROWS_COMPONENTS_SELECT_PERIOD = int(params['components_select_period'] * 24 * 60 * 60 / params['timebar_interval_sec'])
    
    return (ROWS_REBALANCE, ROWS_EXECUTION, ROWS_WAIT_FOR_EXECUTION, ROWS_WEIGHT_CALC, ROWS_COMPONENTS_SWAP_INTERVAL, ROWS_COMPONENTS_SELECT_PERIOD)

def prepare_dataframe(params, disable_tqdm=False):
    # ポートフォリオ構成銘柄の価格系列とボリューム系列を準備する
    _dfs = []
    
    # よく使う定数を計算しておく
    ROWS_REBALANCE, ROWS_EXECUTION, ROWS_WAIT_FOR_EXECUTION, ROWS_WEIGHT_CALC, ROWS_COMPONENTS_SWAP_INTERVAL, ROWS_COMPONENTS_SELECT_PERIOD = calc_cycle_constants(params)

    # TimescaleDBからクローズ系列、ドル単位の取引ボリュームを取得する
    _table_name = TimebarManager.get_table_name()
    _sql = f'SELECT datetime, symbol, close, mark_close, quote_volume from "{_table_name}" WHERE datetime BETWEEN timestamp \'{params["backtest_from"]}\' - interval \'{params["weight_calc_period"] * 24 * 60 * 60} seconds\' AND timestamp \'{params["backtest_to"]}\' ORDER BY datetime ASC'
    df_portfolio = TimescaleDBManager.read_sql_query(_sql, 'TimebarManager')

    # df_portfolioにUSDT/USDTを追加
    _unique_datetime = df_portfolio['datetime'].unique()
    _df_usdt = pd.DataFrame(index=_unique_datetime, columns= ['symbol', 'close', 'quote_volume']).rename_axis('datetime').sort_index().reset_index()
    _df_usdt.loc[:, 'symbol'] = 'USDTUSDT'
    _df_usdt.loc[:, 'close'] = 1.0
    _df_usdt.loc[:, 'mark_close'] = _df_usdt.loc[:, 'close']
    _df_usdt.loc[:, 'quote_volume'] = 0
    df_portfolio = pd.concat([df_portfolio, _df_usdt])

    # デバッグ用のsinusdt系列とsinusdt系列を追加
    if params['debug_sinusdt'] == True:
        # 注 : このsinカーブは起点がシミュレーション開始時間として指定された時間から、ポートフォリオ計算に必要な時間を引いた時点となる
        _unique_datetime = df_portfolio['datetime'].unique()
        _df_sin = pd.DataFrame(index=_unique_datetime, columns= ['symbol', 'close', 'quote_volume']).rename_axis('datetime').sort_index().reset_index()
        _df_sin.loc[:, 'symbol'] = 'sinusdt'
        _df_sin.loc[:, 'close'] = np.sin(2 * np.pi * _df_sin.index.values / (30 * 24 * 60 * 60 / params['timebar_interval_sec'])) / 2 + 1
        _df_sin.loc[:, 'mark_close'] = _df_sin.loc[:, 'close']
        _df_sin.loc[:, 'quote_volume'] = 0
        df_portfolio = pd.concat([df_portfolio, _df_sin])
        
        _df_sin2 = _df_sin.copy()
        _df_sin2.loc[:, 'symbol'] = 'sin2usdt'
        df_portfolio = pd.concat([df_portfolio, _df_sin2])
    
    # indexの設定と並び替え
    df_portfolio.set_index(['symbol', 'datetime'], inplace = True)
    df_portfolio.sort_index(level=['symbol', 'datetime'], inplace = True)
        
    return df_portfolio

# PyPortfolioOptを利用して目標ポートフォリオウェイトを計算 (_df_target_weightを埋める)
def calc_target_weight(df_close, df_target_weight, df_dollar_volume_sma, params):    
    # 銘柄を格納する配列
    _portfolio_components = []
    
    # よく使う定数を計算しておく
    ROWS_REBALANCE, ROWS_EXECUTION, ROWS_WAIT_FOR_EXECUTION, ROWS_WEIGHT_CALC, ROWS_COMPONENTS_SWAP_INTERVAL, ROWS_COMPONENTS_SELECT_PERIOD = calc_cycle_constants(params)
    
    # 初期のUSDT 100%のウェイトを設定
    _rows_early_skip = max(ROWS_WEIGHT_CALC, ROWS_COMPONENTS_SELECT_PERIOD)
    _initial_rebalance = ROWS_REBALANCE * math.ceil(_rows_early_skip / ROWS_REBALANCE)
    df_target_weight.iloc[0:_initial_rebalance] = 0
    df_target_weight.iloc[0:_initial_rebalance, df_target_weight.columns.get_loc('USDTUSDT')] = 1
    
    for i in tqdm(range(_initial_rebalance, df_target_weight.shape[0], ROWS_REBALANCE)):
        # 銘柄入れ替えタイミングか、銘柄配列が空の場合はポートフォリオ銘柄を再計算する
        if i % ROWS_COMPONENTS_SWAP_INTERVAL == 0 or len(_portfolio_components) == 0:
            if params['debug_sinusdt'] == True:
                # デバッグ用に強制的にsinusdt, sin2usdtだけのポートフォリオとする
                _portfolio_components = ['sinusdt', 'sin2usdt']
            else:
                # 取引ボリュームの順位を計算し、10位以内の銘柄名をリストとして抽出する
                _volume_rank = df_dollar_volume_sma.iloc[i].rank(ascending = False)
                _portfolio_components = list(_volume_rank[_volume_rank <= params['num_components']].index.values)

        # ポートフォリオウェイト計算用のクローズ系列を抽出する
        _df_close_window = df_close.iloc[max(0, i - ROWS_WEIGHT_CALC + 1):i + 1, :].loc[:, _portfolio_components]

        # ここよりPyPortfolioOptによるポートフォリオウェイト計算
        _mu = mean_historical_return(_df_close_window)
        _historical_returns = returns_from_prices(_df_close_window).fillna(0)
        
        if params['efficientfrontier_type'] == 'EfficientSemivariance':
            # デフォルトのECOSソルバーではエラーが出るので、SCSを使う
            _ef = EfficientSemivariance(_mu, _historical_returns, solver='SCS', weight_bounds = (-1, 1))
        else:
            _S = CovarianceShrinkage(_historical_returns, returns_data=True).ledoit_wolf()
            _ef = EfficientFrontier(_mu, _S, weight_bounds = (-1, 1))

        if params['l2_reg_gamma'] != 0:
            _ef.add_objective(objective_functions.L2_reg, gamma=params['l2_reg_gamma']) # L2正則化を入れてひとつの銘柄にウェイトが集中するのを防ぐ

        # メモ : 負のポジションを許し、ポジションの絶対値の合計を1以下にするためにpyportfoliooptの_make_weight_sum_constraint関数に変更を加えている
        #def _make_weight_sum_constraint(self, is_market_neutral):
        #    ...
        #    else:
        #        # Check negative bound
        #        negative_possible = np.any(self._lower_bounds < 0)
        #        if negative_possible:
        #            # Use norm1 as position constraint
        #            self.add_constraint(lambda w: cp.sum(cp.abs(w)) <= 1)
        #        else: 
        #            self.add_constraint(lambda w: cp.sum(w) == 1)
        #    self._market_neutral = is_market_neutral

        # 目標ウェイトを計算する
        df_target_weight.iloc[i, :] = 0
        try:
            if params['objective_type'] == 'max_quadratic_utility':
                _weights = _ef.max_quadratic_utility(risk_aversion = params['objective_param'])
            elif params['objective_type'] == 'max_sharpe':
                _weights = _ef.max_sharpe()
            elif params['objective_type'] == 'min_volatility':
                if params['efficientfrontier_type'] == 'EfficientSemivariance':
                    _weights = _ef.min_semivariance()
                else:
                    _weights = _ef.min_volatility()
            elif params['objective_type'] == 'efficient_risk':
                if params['efficientfrontier_type'] == 'EfficientSemivariance':
                    _weights = _ef.efficient_risk(target_semideviation = params['objective_param'])
                else:
                    _weights = _ef.efficient_risk(target_volatility = params['objective_param'])
            elif params['objective_type'] == 'efficient_return':
                _weights = _ef.efficient_return(target_return = params['objective_param'])
            
            # 目標ウェイトを書き込み
            _cleaned_weights = _ef.clean_weights()
        except BaseException as e:
            # 例外発生時にはUSDTUSDTにすべてのウェイトを割り当てる
            print(f'{datetime.now(timezone.utc)} : ExchangeManager.calc_target_weight() : Assign 100% to USDT. Exception {e}')
            _cleaned_weights = { 'USDTUSDT': 1.0 }

        if params['debug_sinusdt'] == True:
            if (i // ROWS_REBALANCE) % 2 == 0:
                df_target_weight.iloc[i, df_target_weight.columns.get_loc('sinusdt')] = -1
            else:
                df_target_weight.iloc[i, df_target_weight.columns.get_loc('sin2usdt')] = -1
        else:
            for _key, _value in _cleaned_weights.items():
                df_target_weight.iloc[i, df_target_weight.columns.get_loc(_key)] = _value

        # 目標ウェイトを合計して1にならない場合があるので、補正を行う
        _target_weights = df_target_weight.iloc[i]

        if np.count_nonzero(_target_weights) <= 1:
            # 構成銘柄が1つしかない場合は、足りないウェイトをUSDT/USDTに足す
            df_target_weight.iloc[i, df_target_weight.columns.get_loc('USDTUSDT')] += (1 - _target_weights.abs().sum())
        elif _target_weights.abs().sum() != 1:
            # 構成銘柄が複数ある場合は、通常の正規化を行う
            df_target_weight.iloc[i, :] = _target_weights /_target_weights.abs().sum()
        
        # 執行を待っている間はターゲットウェイトを維持するよう書き込む
        df_target_weight.iloc[i:i + ROWS_WAIT_FOR_EXECUTION - 1, :] = df_target_weight.iloc[i, :]
    
    # 執行期間にある行の目標ウェイトを、その直後の目標ウェイトで埋める
    df_target_weight.bfill(inplace = True)
    
    # 最後のリバランスタイミングからデータフレームの末尾までを、最後の目標ウェイトで埋める
    df_target_weight.ffill(inplace = True)
    
    # リバランス中を含むすべてのタイミングで1に満たないウェイトを、USDT/USDTに割り当てる
    df_target_weight.loc[:, 'USDTUSDT'] += (1 - df_target_weight.abs().sum(axis = 1))
    
    # 異常なウェイトがないか確認
    if params['debug'] == True:
        _abssize = df_target_weight.abs().sum(axis = 1)
        if (_abssize > 1.001).any():
            raise 'abssize > 1'
        if (_abssize < 0.999).any():
            raise 'abssize < 1'
    
    return df_target_weight

def simulate_trades(df_close, df_mark_close, df_target_weight, params):
    # よく使う定数を計算しておく
    ROWS_REBALANCE, ROWS_EXECUTION, ROWS_WAIT_FOR_EXECUTION, ROWS_WEIGHT_CALC, ROWS_COMPONENTS_SWAP_INTERVAL, ROWS_COMPONENTS_SELECT_PERIOD = calc_cycle_constants(params)
    
    # シミュレーションの結果を格納するDataframeを用意する
    df_real_weight = df_target_weight.copy()
    df_usdt_value = df_target_weight.copy()
    df_position = df_target_weight.copy()
    df_fee = df_target_weight.copy()
    
    # 初期リアルウェイト、初期ポジション、初期USDT価値を設定してシミュレーションに備える (target_weightはすでに設定済み)
    df_real_weight.iloc[0, df_position.columns.get_loc('USDTUSDT')] = 1
    df_position.iloc[0, df_position.columns.get_loc('USDTUSDT')] = params['initial_usdt_value']
    df_usdt_value.iloc[0, df_position.columns.get_loc('USDTUSDT')] = params['initial_usdt_value']
    
    # t+1のオープン価格を内部で利用しているので、最終行はシミュレーションに含めない
    for i in tqdm(range(1, df_target_weight.shape[0] - 1, 1)):
        np_real_weight, np_usdt_value, np_fee, np_position, = simulate_trades_numba(i, df_close.values, df_mark_close.values, df_target_weight.values, df_real_weight.values, df_usdt_value.values, df_position.values, df_fee.values, ROWS_REBALANCE, ROWS_WAIT_FOR_EXECUTION, ROWS_EXECUTION, df_close.columns.get_loc('USDTUSDT'), params['execution_cost'])
        df_real_weight.iloc[i, :] = np_real_weight
        df_usdt_value.iloc[i, :] = np_usdt_value
        df_fee.iloc[i, :] = np_fee
        df_position.iloc[i, :] = np_position
    
    return (df_real_weight.iloc[:-1], df_usdt_value.iloc[:-1], df_fee.iloc[:-1], df_position.iloc[:-1])

# 5分足でのリバランス処理の計算を行う (numba版)
# 用語 : リバランスサイクル = 2時間～数日間で行われるポートフォリオウェイトの大きな調整
#        リバランスステップ = リバランスはサイクルを小さく分割して少しずつ行う。その分割されたステップのこと。おおまかに5分ごとに行われる。
#@numba.jit(nopython=True)
def simulate_trades_numba(i, np_close, np_mark_close, np_target_weight, np_real_weight, np_usdt_value, np_position, np_fee, ROWS_REBALANCE, ROWS_WAIT_FOR_EXECUTION, ROWS_EXECUTION, idx_usdt, execution_cost):
    _step_in_rebalance_cycle = i % ROWS_REBALANCE

    # ステップt-1とステップtのクローズ価格の差を計算する。新規銘柄が取引所に追加されたタイミングではNaNが出てくるので0クリアしておく
    _close_diff = np_close[i] - np_close[i-1]
    _close_diff[np.isnan(_close_diff)] = 0

    # マーク価格ベースでシミュレーションしたい場合に利用する
    _mark_close_diff = np_mark_close[i] - np_mark_close[i-1]
    _mark_close_diff[np.isnan(_mark_close_diff)] = 0
    
    # ステップt-1の保有ポジションがステップtのクローズ価格をもとにすると、どれだけのUSDT建て価値になるかを求める
    # 具体的には、ステップt-1での保有ポジション x ステップt-1とステップtのクローズ価格の差 を ステップt-1のUSDT建て価値に加算する
    # ステップ0はUSDT/USDTの割合が100%、USDT建て価値として初期値10,000が入っている
    _value_before_rebalance = np_usdt_value[i-1] + np.sign(np_position[i-1]) * _mark_close_diff * np_position[i-1]
    _value_before_rebalance[np.isnan(_value_before_rebalance)] = 0
    _value_before_rebalance_abssum = np.sum(np.abs(_value_before_rebalance))
    
    # ステップt-1の保有ポジションが現在USDT建てでどれだけの価値かをもとに、このステップの売買による調整を行う前のポートフォリオウェイトを求める
    _np_real_weight_before_rebalance = _value_before_rebalance / _value_before_rebalance_abssum
    
    if _step_in_rebalance_cycle < ROWS_WAIT_FOR_EXECUTION or _step_in_rebalance_cycle >= ROWS_WAIT_FOR_EXECUTION + ROWS_EXECUTION:
        # 5分足確定直後の取引休止区間、あるいはウェイト調整後は、直前の保有ポジションを維持するだけ
        np_position[i] = np_position[i-1]
        np_usdt_value[i] = _value_before_rebalance
        np_real_weight[i] = _np_real_weight_before_rebalance
        np_fee[i] = 0
    else:
        # リバランスサイクルの中の執行期間に入っているので、このステップ終了時の目標ウェイトを求める。np_target_weight[i]はリバランスサイクルが終わる際の目標ウェイトが入っている
        # 1/残りリバランスステップ数ずつウェイトを変えていく
        np_real_weight[i] = _np_real_weight_before_rebalance + (np_target_weight[i] - _np_real_weight_before_rebalance) / (ROWS_EXECUTION + ROWS_WAIT_FOR_EXECUTION - _step_in_rebalance_cycle)

        # 線形補間なので絶対値の合計が1以下になるケースがある。資産が減ってしまうので、不足分をUSDT/USDTに割り当てる
        _real_weight_abssum = np.sum(np.abs(np_real_weight[i]))
        np_real_weight[i][idx_usdt] += 1 - _real_weight_abssum        
        
        # このサイクルが終わった時点で到達しているべき、各銘柄のUSDT建ての価値を求める (この段階ではまだポジション調整の手数料を考慮していない)
        np_usdt_value[i] = _value_before_rebalance_abssum * np_real_weight[i]
        
        # ステップt-1から現在ステップでのUSDT建ての価値の変化量から、徴収される手数料の絶対値を求める。USDTは手数料は常に0
        _usdt_value_diff_abs = np.abs(np_usdt_value[i] - np_usdt_value[i-1])
        _usdt_value_diff_abs[np.isnan(_usdt_value_diff_abs)] = 0
        np_fee[i] = _usdt_value_diff_abs * execution_cost
        np_fee[i][idx_usdt] = 0

        # 手数料の合計を_value_before_rebalance_abssumから引き算し、手数料を反映したリバランス後のUSDT建てのポートフォリオ価値を求める
        _value_before_rebalance_after_fee_abssum = _value_before_rebalance_abssum - np.sum(np_fee[i])
        
        # このサイクルが終わった時点で到達しているべき、各銘柄のUSDT建ての価値を求める (ポジション調整の手数料を考慮したもの)
        np_usdt_value[i] = _value_before_rebalance_after_fee_abssum * np_real_weight[i]

        # ステップtでの目標USDT建て価値と、ステップtのクローズ価格から、ステップtのポジションを計算する
        _np_position = np_usdt_value[i] / np_close[i]
        _np_position[np.isnan(_np_position)] = 0
        np_position[i] = _np_position
        
    return (np_real_weight[i], np_usdt_value[i], np_fee[i], np_position[i])

from plotly.subplots import make_subplots
import plotly.graph_objects as go
import plotly.express as px
import seaborn as sns

# Seabornのパレットを取得する
def get_colorpalette(colorpalette, n_colors):
    palette = sns.color_palette(colorpalette, n_colors)
    rgba = [f'rgba({int(rgb[0] * 255)}, {int(rgb[1] * 255)}, {int(rgb[2] * 255)}, 1.0)' for rgb in palette]
    return rgba

# パフォーマンス表示とmlflow記録用の関数
def visualize_performance_plotly(df_close, df_target_weight, df_real_weight, df_usdt_value, df_position, df_fee, render_from, render_to, time_resolution, record_metric, params):
    # Jupyter notebookで表示したい場合はこちらを使う
    _fig = make_subplots(rows = 3, cols = 1,
                         subplot_titles = ('未実現損益 + 証拠金', 'クローズ価格', '実際ポートフォリオウェイト'),
                         shared_xaxes = True,
                         vertical_spacing=0.04)
    
    # レンダリング対象のウィンドウを作る
    _df_close = df_close.loc[render_from:render_to, :]
    _df_target_weight = df_target_weight.loc[render_from:render_to, :]
    _df_real_weight = df_real_weight.loc[render_from:render_to, :]
    _df_usdt_value = df_usdt_value.loc[render_from:render_to, :]
    _df_position = df_position.loc[render_from:render_to, :]
    _df_fee = df_fee.loc[render_from:render_to, :]

    
    # 描画中にアクティブな銘柄をリスト化しておく
    _series_df_real_weight_active_columns = df_real_weight.fillna(0).astype(bool).sum(axis=0)
    _series_df_real_weight_active_columns = _series_df_real_weight_active_columns[_series_df_real_weight_active_columns != 0]
    
    _series_df_target_weight_active_columns = df_target_weight.fillna(0).astype(bool).sum(axis=0)
    _series_df_target_weight_active_columns = _series_df_target_weight_active_columns[_series_df_target_weight_active_columns != 0]
    
    _columns = set(_series_df_real_weight_active_columns.index.values) | set(_series_df_target_weight_active_columns.index.values)
    
    # カラーパレットの取得
    _colors = get_colorpalette('hls', len(_columns))

    _legend_list = []
    
    # ポジション価値合計の描画
    _df_usdt_total_value = _df_usdt_value.abs().sum(numeric_only = True, axis = 1)
    _df_usdt_value_render = _df_usdt_total_value.loc[(_df_usdt_total_value.index.values.astype(np.int64) // 10**9) % (time_resolution * 60) == 0]
    _fig.add_trace(go.Scatter(x = _df_usdt_value_render.index, y = _df_usdt_value_render.iloc[:], name = 'USDT value', line = {'width': 1.5, 'color': _colors[0]}), row = 1, col = 1)
    
    # クローズ系列の描画
    _df_close_render = _df_close.loc[(_df_close.index.values.astype(np.int64) // 10**9 % (time_resolution * 60) == 0), list(_columns)]
    for i, _col in enumerate(_columns):
        if _col in _df_close_render:
            if _col not in _legend_list:
                _legend_list.append(_col)
                _show_legend = True
            else:
                _show_legend = False

            if params['debug'] == True:
                pass
            else:
                _df_close_render = _df_close_render / _df_close_render.bfill(axis=0).iloc[0, :]
                _fig.update_yaxes(type="log", row=2, col=1)

            _fig.add_trace(go.Scatter(x = _df_close_render.index, y = _df_close_render[_col], name = _col, line = {'width': 1, 'color': _colors[i]}, showlegend = _show_legend), row = 2, col = 1)

    # リアルポートフォリオウェイトの描画
    _df_real_weight_render = _df_real_weight.loc[(_df_real_weight.index.values.astype(np.int64) // 10**9) % (time_resolution * 60) == 0, list(_columns)]
    _df_real_weight_render_negative, _df_real_weight_render_positive = _df_real_weight_render.clip(upper=0), _df_real_weight_render.clip(lower=0)
    for i, _col in enumerate(_columns):
        if _col in _df_real_weight_render_negative:
            if _col not in _legend_list:
                _legend_list.append(_col)
                _show_legend = True
            else:
                _show_legend = False
            _fig.add_trace(go.Scatter(x = _df_real_weight_render_negative.index, y = _df_real_weight_render_negative[_col], name = _col, stackgroup = 'negative', mode = 'none', fillcolor = _colors[i], showlegend = _show_legend), row = 3, col = 1)

    for i, _col in enumerate(_columns):
        if _col in _df_real_weight_render_positive:
            if _col not in _legend_list:
                _legend_list.append(_col)
                _show_legend = True
            else:
                _show_legend = False
            _fig.add_trace(go.Scatter(x = _df_real_weight_render_positive.index, y = _df_real_weight_render_positive[_col], name = _col, stackgroup = 'positive', mode = 'none', fillcolor = _colors[i], showlegend = _show_legend), row = 3, col = 1)
        
    _fig.update_layout(width = 700, height = 1000)

    mlflow.log_figure(_fig, f'portofolio_{render_from}_to_{render_to}.html')
    
    # 最終・最大価値の計算
    final_usdt_value = _df_usdt_total_value.iloc[-1]
    max_usdt_value = _df_usdt_total_value.max()
    
    # シャープレシオの計算
    _returns = (np.log1p(_df_usdt_total_value) - np.log1p(_df_usdt_total_value.shift()))
    mean_return = _returns.mean()
    std_return = np.std(_returns)
    try:
        sharpe = mean_return / std_return * math.sqrt(365 * 24 * 60 * 60 / params['timebar_interval_sec'])
    except BaseException as e:
        sharpe = -10000
    
    # DD値の計算
    _dd_val = _df_usdt_total_value.cummax() - _df_usdt_total_value
    _dd_end = _dd_val.idxmax()
    _dd_start = _df_usdt_total_value.cummax()[:_dd_end].idxmax()
    dd_pct = 1 - (_df_usdt_total_value.loc[_dd_end] / _df_usdt_total_value.loc[_dd_start])
        
    if record_metric == True:
        mlflow.log_metric(key="max_value", value = max_usdt_value)
        mlflow.log_metric(key="final_value", value = final_usdt_value)
        mlflow.log_metric(key='sharpe', value = sharpe)
        mlflow.log_metric(key='mean_return', value = mean_return)
        mlflow.log_metric(key='std_return', value = std_return)
        mlflow.log_metric(key="dd_value", value = _dd_val.loc[_dd_end])
        mlflow.log_metric(key="dd_pct", value = dd_pct * 100)
        mlflow.log_metric(key="total fee", value = _df_fee.abs().sum().sum())
    
    return final_usdt_value, max_usdt_value, dd_pct, sharpe

# シャープレシオを最大化するOptunaの最適化対象の関数
def target_function(params):
    # パラメータを表示する
    print(params)
    
    # パラメータをmlflowのrunに記録
    mlflow.end_run()
    for _idx, _value in enumerate(params):
        mlflow.log_param(_value, params[_value])

    _df_portfolio = pd.read_pickle('df_portfolio.pkl')
    
    # よく使う定数を計算しておく
    ROWS_REBALANCE, ROWS_EXECUTION, ROWS_WAIT_FOR_EXECUTION, ROWS_WEIGHT_CALC, ROWS_COMPONENTS_SWAP_INTERVAL, ROWS_COMPONENTS_SELECT_PERIOD = calc_cycle_constants(params)
    
    # ポートフォリオ計算に利用するクローズとボリューム用のデータフレームを準備
    _df_close = _df_portfolio.reset_index().pivot(index = 'datetime', columns = 'symbol', values='close')
    _df_mark_close = _df_portfolio.reset_index().pivot(index = 'datetime', columns = 'symbol', values='mark_close')
    _df_dollar_volume = _df_portfolio.reset_index().pivot(index = 'datetime', columns = 'symbol', values='quote_volume').fillna(0)
    _df_dollar_volume_sma = _df_dollar_volume.apply(lambda rows: talib.SMA(rows, ROWS_COMPONENTS_SELECT_PERIOD))
    _df_target_weight = _df_close.copy()
    _df_target_weight[:] = np.nan # ffillとbfillを使うので、np.nanで埋める必要がある

    _df_target_weight = calc_target_weight(_df_close, _df_target_weight, _df_dollar_volume_sma, params)
    _df_real_weight, _df_usdt_value, _df_fee, _df_position = simulate_trades(_df_close, _df_mark_close, _df_target_weight, params)
    
    # Plotlyを利用した可視化
    visualize_performance_plotly(_df_close, _df_target_weight, _df_real_weight, _df_usdt_value, _df_position, _df_fee,
                                                                                        '2022-05-08 00:00:00+00', params['backtest_to'], 10, False, params)
    _final_usdt_value, _max_usdt_value, _dd_pct, _sharpe = visualize_performance_plotly(_df_close, _df_target_weight, _df_real_weight, _df_usdt_value, _df_position, _df_fee,
                                                                                        params['backtest_from'], '2022-05-08 00:00:00+00', 10, True, params)
    
    gc.collect()
    mlflow.end_run()
    
    return _final_usdt_value, _max_usdt_value, _dd_pct, _sharpe
    
# Optunaの最適化目的関数
def objective(trial):
    # 実験用パラメータの設定
    global params
    _params = params.copy()
    
    if _params['debug'] == False:
        _params['rebalance_interval_hour'] = trial.suggest_int('rebalance_interval_hour', 8, 24, 8) # リバランス間隔の最小値を1時間にすると、シミュレーション時間が長くなりすぎる        
        _params['rebalance_time_sec'] = _params['rebalance_interval_hour'] * 60 * 60 / 8 # リバランス間隔の1/4の時間でウェイト調整を終える
        _params['objective_param'] = trial.suggest_uniform('risk_aversion', 0.1, 4.0)
        _params['l2_reg_gamma'] = trial.suggest_uniform('l2_reg_gamma', 0.01, 0.1)
        _params['num_components'] = trial.suggest_int('num_components', 4, 20, 2)
        _params['weight_calc_period'] = trial.suggest_int('weight_calc_period', 2, 8, 2)
        _params['components_select_period'] = _params['weight_calc_period']
            
    _final_usdt_value, _max_usdt_value, _dd_pct, _sharpe = target_function(_params)
    return _max_usdt_value, _sharpe

# コマンドライン引数の取得
parser = argparse.ArgumentParser()
parser.add_argument('-d', '--debug', help = 'Turn debug flag on', action = 'store_true')
parser.add_argument('-ds', '--debugsinusdt', help = 'Use sinusdt artificial price seriesdebug flag on', action = 'store_true')
parser.add_argument('-df', '--dataframe', help = 'Recreate dataframe pickle file', action = 'store_true')

args = parser.parse_args()

# 実験のパラメータ
params_base = {
    'experiment_name': 'bugfix4_int_8h-24h_cost_0.006_l2reg_0.01-0.1_riska_0.1_4',
    'backtest_from': '2022-04-08 00:00:00+00', # Binance testnetは2021年8月以前の値動きが激しすぎるので除外
    'backtest_to': '2023-01-01 00:00:00+00',
    'efficientfrontier_type': 'EfficientMeanVariance',
    'objective_type': 'max_quadratic_utility',
    'l2_reg_gamma': 0.03,
    'execution_cost': 0.006,                 # トレード手数料
    'debug': args.debug,                     # 規定値のパラメータを使ったデバッグを行うフラグ
    'debug_sinusdt': args.debugsinusdt,      # 価格系列としてsinusdt / sin2usdtを利用するフラグ
    'exchange_name': 'binanceusdm-mainnet', # DB読み込み時に利用される取引所名
#    'exchange_name': 'binanceusdm-testnet',         # DB読み込み時に利用される取引所名
    'initial_usdt_value': 2000,              # USDT/USDTの初期ポジション数
    'experiment_rounds': 100,                # 実験の繰り返し数 
    'timebar_interval_sec': 5*60,            # タイムバー間隔 [秒]
    
    'objective_param': 3,        # リスク回避度
    'num_components': 16,           # ポートフォリオ銘柄数
    'rebalance_interval_hour': 4,   # ポートフォリオリバランス間隔 [時間]
    'rebalance_wait_sec': 5 * 60,   # ポートフォリオリバランス開始までの待ち時間
    'rebalance_time_sec': 60 * 60,  # ポートフォリオリバランス執行の期間 (5分に1回の執行を行う)
    'weight_calc_period': 2,        # ポートフォリオウェイト計算時のリターン系列の長さ [日]
    'components_swap_interval': 1,  # ポートフォリオ銘柄入替間隔 [ポートフォリオリバランス回数, 1の場合は毎回入れ替え, 2の場合は2回ごとに入れ替え]
    'components_select_period': 2, # ポートフォリオ銘柄ボリューム観察期間 [日]
}

# 実験名を設定
experiment_name = params_base['exchange_name'] + '_' + params_base['experiment_name']

if params_base['debug'] == True:
    experiment_name += '_debug'
    params_base['experiment_rounds'] = 1

# mlflowの設定
mlflow.set_tracking_uri('http://mlflow:8890')

# 実験用パラメータの初期化
params = params_base.copy()

if params['debug'] == True:
    params['backtest_from'] = '2022-04-03 00:00:00+00'
    params['backtest_to'] = '2023-01-01 00:00:00+00'
    params['rebalance_interval_hour'] = 8
    params['rebalance_wait_sec'] = 5*60
    params['rebalance_time_sec'] = 2*60*60
    params['weight_calc_period'] = 2
    params['num_components'] = 16
    params['components_swap_interval'] = 1
    params['components_select_period'] = 2        
    params['initial_usdt_value'] = 10000
    params['execution_cost'] = 0.006
    
# AsyncManagerの初期化
_richhandler = RichHandler(rich_tracebacks = True)
_richhandler.setFormatter(logging.Formatter('%(message)s'))
basicConfig(level = logging.INFO, datefmt = '[%Y-%m-%d %H:%M:%S]', handlers = [_richhandler])
_logger: Logger = getLogger('rich')
AsyncManager.set_logger(_logger)

# TimebarManagerの初期化前に、TimescaleDBManagerの初期化が必要
TimescaleDBManager(pg_config)

# TimebarManagerの初期化前に、PyBottersManagerの初期化が必要
_exchange_config = binance_config.copy()
_pybotters_params = _exchange_config.copy()
_pybotters_params['apis'] = pybotters_apis.copy()
PyBottersManager(_pybotters_params)

# タイムバーをダウンロードするだけなら、run_asyncを読んでWebsocket APIからポジション情報等をダウンロードする必要はない
ExchangeManager(_exchange_config)

# TimebarManagerの初期化
_timebar_params = {
    'timebar_interval': timedelta(minutes = 5)
}
TimebarManager(_timebar_params)

if args.dataframe:
    df_portfolio = prepare_dataframe(params, False)

    # 初期状態のデータフレームをファイルに保存する
    df_portfolio.to_pickle('df_portfolio.pkl')
else:
    # 保存済みのデータフレームを読み込む
    df_portfolio = pd.read_pickle('df_portfolio.pkl')

# Optunaを利用してパラメータ最適化を行う
mlflow.set_experiment(experiment_name)
_study = optuna.create_study(directions = ['maximize', 'maximize'], study_name = experiment_name,
                             storage = 'postgresql+psycopg2://wannabebotter:wannabebotter@timescaledb:5432/optuna', load_if_exists=True)
_study.optimize(objective, n_trials = params_base['experiment_rounds'] )