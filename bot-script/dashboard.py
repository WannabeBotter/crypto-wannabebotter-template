import os
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pandas as pd
import numpy as np

from dash import Dash, html, dcc
from dash.dependencies import Input, Output
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import seaborn as sns

from timescaledb_manager import TimescaleDBManager

from crypto_bot_config import pg_config, binance_testnet_config, binance_config, pybotters_apis

# Seabornのパレットを取得する
def get_colorpalette(colorpalette, n_colors):
    palette = sns.color_palette(colorpalette, n_colors)
    rgba = [f'rgba({int(rgb[0] * 255)}, {int(rgb[1] * 255)}, {int(rgb[2] * 255)}, 1.0)' for rgb in palette]
    return rgba

# パフォーマンス表示とmlflow記録用の関数
def visualize_performance_plotly(df_target_weight, df_real_weight, df_upnl, df_rpnl, render_from, render_to):
    # Jupyter notebookで表示したい場合はこちらを使う
    _fig = make_subplots(rows = 2, cols = 2,
                         subplot_titles = ('未実現損益 + 証拠金', '実際ウェイト', '未実現損益 + 実現損益', '目標ウェイト'),
                         shared_xaxes = True,
                         vertical_spacing=0.04)
    
    # レンダリング対象のウィンドウを作る
    _df_target_weight = df_target_weight.loc[render_from:render_to, df_target_weight.any()]
    _df_real_weight = df_real_weight.loc[render_from:render_to, df_real_weight.any()]
    _df_upnl = df_upnl.loc[render_from:render_to, df_upnl.any()].fillna(0)
    _df_rpnl = df_rpnl.loc[render_from:render_to, df_rpnl.any()].cumsum()

    # 描画中にアクティブな銘柄をリスト化しておく
    _series_df_real_weight_active_columns = df_real_weight.fillna(0).astype(bool).sum(axis=0)
    _series_df_real_weight_active_columns = _series_df_real_weight_active_columns[_series_df_real_weight_active_columns != 0]
    
    _series_df_target_weight_active_columns = df_target_weight.fillna(0).astype(bool).sum(axis=0)
    _series_df_target_weight_active_columns = _series_df_target_weight_active_columns[_series_df_target_weight_active_columns != 0]
    
    _series_df_usdt_pnl_active_columns = df_upnl.fillna(0).astype(bool).sum(axis=0)
    _series_df_usdt_pnl_active_columns = _series_df_usdt_pnl_active_columns[_series_df_usdt_pnl_active_columns != 0]

    _columns = set(_series_df_real_weight_active_columns.index.values) | set(_series_df_target_weight_active_columns.index.values) | set(_series_df_usdt_pnl_active_columns.index.values)
    _columns = _columns - {'cw_usdt_balance'}
    _columns = sorted(_columns)
    
    # カラーパレットの取得
    _colors = get_colorpalette('hls', len(_columns))
    
    _legend_list = []

    # ポジション価値合計の描画
    _df_usdt_total_value = _df_upnl.sum(numeric_only = True, axis = 1)
    _df_usdt_value_render = _df_usdt_total_value
    _fig.add_trace(go.Scatter(x = _df_usdt_value_render.index, y = _df_usdt_value_render.iloc[:], name = 'USDT value', line = {'width': 1.5, 'color': _colors[0]}), row = 1, col = 1)

    # 通貨ごとの未実現PnL+実現PnLの描画
    _idx_diff = _df_upnl.index.difference(_df_rpnl.index)
    _df_rpnl = pd.concat([_df_rpnl, pd.DataFrame(index = _idx_diff)])
    _df_rpnl.sort_index(inplace = True)
    _column_diff = _df_upnl.columns.difference(_df_rpnl.columns)
    _df_rpnl.loc[:, _column_diff] = 0
    _df_rpnl.fillna(method='ffill', inplace = True)
    _df_rpnl.fillna(0, inplace = True)
    
    _idx_diff = _df_rpnl.index.difference(_df_upnl.index)
    _df_rpnl.drop(_idx_diff, axis=0, inplace = True)
    
    for i, _col in enumerate(_columns):
        if _col in _df_upnl.columns.values and _df_upnl.loc[:, _col].any():
            if _col not in _legend_list:
                _legend_list.append(_col)
                _show_legend = True
            else:
                _show_legend = False
            _fig.add_trace(go.Scatter(x = _df_upnl.index, y = _df_upnl[_col] + _df_rpnl[_col], name = _col, line = {'width': 1.5, 'color': _colors[i]}, showlegend = _show_legend), row = 2, col = 1)
            
    #for i, _col in enumerate(_columns):
    #    if _col in _df_usdt_pnl_render_negative.columns.values:
    #        if _col not in _legend_list:
    #            _legend_list.append(_col)
    #            _show_legend = True
    #        else:
    #            _show_legend = False
    #        _fig.add_trace(go.Scatter(x = _df_usdt_pnl_render_negative.index, y = _df_usdt_pnl_render_negative[_col], name = _col, stackgroup = 'negative', mode = 'none', fillcolor = _colors[i], showlegend = _show_legend), row = 2, col = 1)
    #        
    #    if _col in _df_usdt_pnl_render_positive.columns.values:
    #        if _col not in _legend_list:
    #            _legend_list.append(_col)
    #            _show_legend = True
    #        else:
    #            _show_legend = False
    #        _fig.add_trace(go.Scatter(x = _df_usdt_pnl_render_positive.index, y = _df_usdt_pnl_render_positive[_col], name = _col, stackgroup = 'positive', mode = 'none', fillcolor = _colors[i], showlegend = _show_legend), row = 2, col = 1)

    # 実際ポートフォリオウェイトの描画
    _df_real_weight_render = _df_real_weight
    _df_real_weight_render_negative, _df_real_weight_render_positive = _df_real_weight_render.clip(upper=0), _df_real_weight_render.clip(lower=0)
        
    for i, _col in enumerate(_columns):
        if _col in _df_real_weight_render_negative.columns.values and len(_df_real_weight_render_negative.loc[:, _col].to_numpy().nonzero()) != 0:
            if _col not in _legend_list:
                _legend_list.append(_col)
                _show_legend = True
            else:
                _show_legend = False
            _fig.add_trace(go.Scatter(x = _df_real_weight_render_negative.index, y = _df_real_weight_render_negative[_col], name = _col, stackgroup = 'negative', mode = 'none', fillcolor = _colors[i], showlegend = _show_legend), row = 1, col = 2)
            
        if _col in _df_real_weight_render_positive.columns.values and len(_df_real_weight_render_positive.loc[:, _col].to_numpy().nonzero()) != 0:
            if _col not in _legend_list:
                _legend_list.append(_col)
                _show_legend = True
            else:
                _show_legend = False
            _fig.add_trace(go.Scatter(x = _df_real_weight_render_positive.index, y = _df_real_weight_render_positive[_col], name = _col, stackgroup = 'positive', mode = 'none', fillcolor = _colors[i], showlegend = _show_legend), row = 1, col = 2)
        
    # 目標ポートフォリオウェイトの描画
    _df_target_weight_render = _df_target_weight
    _df_target_weight_render_negative, _df_target_weight_render_positive = _df_target_weight_render.clip(upper=0), _df_target_weight_render.clip(lower=0)
    
    for i, _col in enumerate(_columns):
        if _col in _df_target_weight_render_negative.columns.values and len(_df_target_weight_render_negative.loc[:, _col].to_numpy().nonzero()) != 0:
            if _col not in _legend_list:
                _legend_list.append(_col)
                _show_legend = True
            else:
                _show_legend = False
            _fig.add_trace(go.Scatter(x = _df_target_weight_render_negative.index, y = _df_target_weight_render_negative[_col], name = _col, stackgroup = 'negative', mode = 'none', fillcolor = _colors[i], showlegend = _show_legend), row = 2, col = 2)
        if _col in _df_target_weight_render_positive.columns.values and len(_df_target_weight_render_positive.loc[:, _col].to_numpy().nonzero()) != 0:
            if _col not in _legend_list:
                _legend_list.append(_col)
                _show_legend = True
            else:
                _show_legend = False
            _fig.add_trace(go.Scatter(x = _df_target_weight_render_positive.index, y = _df_target_weight_render_positive[_col], name = _col, stackgroup = 'positive', mode = 'none', fillcolor = _colors[i], showlegend = _show_legend), row = 2, col = 2)
        
        _fig.update_layout(width = 1200, height = 800, uirevision='0')
    return _fig    

# TimescaleDB用のユーティリティライブラリの初期化
TimescaleDBManager(pg_config)
TimescaleDBManager.init_database('TradeManager')
TimescaleDBManager.init_database('ExchangeManager')

app = Dash(__name__)

app.layout = html.Div([
        html.H1('Portfolio Botのダッシュボード的なもの'),
        html.Div(id='live-update-text'),
        dcc.Graph(id='live-update-graph'),
        dcc.Interval(
            id='interval-component',
            interval=30*1000, # in milliseconds
            n_intervals=0)])

@app.callback(Output('live-update-text', 'children'),
              Input('interval-component', 'n_intervals'))
def update_metrics(n):
    return [
        html.Span(f'更新回数 : {n}')
    ]

def get_value_dataframe(table_name: str = None, datetime_from: datetime = None):
    assert table_name is not None
    assert datetime_from is not None

    _sql = f'SELECT * from "{table_name}" WHERE datetime > \'{datetime_from}\' ORDER BY datetime ASC'
    _df = TimescaleDBManager.read_sql_query(_sql, 'TradeManager', index_column = '')
    _df = _df.pivot(index = 'datetime', columns = 'symbol', values = 'value').astype(float)
    return _df.sort_index(axis = 1)

def get_rpnl_dataframe(table_name: str = None, datetime_from: datetime = None):
    assert table_name is not None
    assert datetime_from is not None

    _sql = f'SELECT datetime, symbol, realized_profit from "{table_name}" WHERE datetime > \'{datetime_from}\' ORDER BY datetime ASC'
    _df = TimescaleDBManager.read_sql_query(_sql, 'ExchangeManager', index_column = '')
    _df['symbol'] = _df['symbol'].str.upper()
    _df = _df.groupby(['datetime', 'symbol'], as_index = False).sum()
    _df = _df.pivot(index = 'datetime', columns = 'symbol', values = 'realized_profit').astype(float)
    return _df.sort_index(axis = 1)

@app.callback(Output('live-update-graph', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_graph_live(n):
    _datetime_to = datetime.now(tz = timezone.utc)
    _datetime_from = _datetime_to - timedelta(days = 7)

    # リアルウェイト取得
    _table_name = f'binanceusdm-mainnet_current_weight_5m'
    df_real_weight = get_value_dataframe(_table_name, _datetime_from)

    # 目標ウェイト取得
    _table_name = f'binanceusdm-mainnet_target_weight_5m'
    df_target_weight = get_value_dataframe(_table_name, _datetime_from)

    # 未実現損益取得
    _table_name = f'binanceusdm-mainnet_current_upnl_5m'
    df_current_upnl = get_value_dataframe(_table_name, _datetime_from)

    # 手数料取得
    _table_name = f'binanceusdm-mainnet_order_log'
    df_rpnl = get_rpnl_dataframe(_table_name, _datetime_from)

    return visualize_performance_plotly(df_target_weight, df_real_weight, df_current_upnl, df_rpnl, _datetime_from, _datetime_to)

if __name__ == '__main__':
    app.run_server(debug=True, host = '0.0.0.0', port = '8050')