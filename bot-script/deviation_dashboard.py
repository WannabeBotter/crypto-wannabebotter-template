import asyncio
from datetime import timedelta, datetime, timezone
from threading import Thread

from async_manager import AsyncManager
from timescaledb_manager import TimescaleDBManager
from pybotters_manager import PyBottersManager
from exchange_manager import ExchangeManager
from timebar_manager import TimebarManager

from crypto_bot_config import pg_config, binance_testnet_config, binance_config, pybotters_apis

from dash import Dash, html, dcc
from dash.dependencies import Input, Output

from plotly.subplots import make_subplots
import plotly.graph_objects as go
import seaborn as sns

from logging import Logger, getLogger, basicConfig, Formatter
import logging
from rich.logging import RichHandler

# AsyncManagerの初期化
_richhandler = RichHandler(rich_tracebacks = True)
_richhandler.setFormatter(logging.Formatter('%(message)s'))
basicConfig(level = logging.DEBUG, datefmt = '[%Y-%m-%d %H:%M:%S]', handlers = [_richhandler])
_logger: Logger = getLogger('rich')
AsyncManager.set_logger(_logger)

# TimescaleDBManagerの初期化
TimescaleDBManager._instance = TimescaleDBManager(pg_config)
_exchangeman = ExchangeManager(binance_config)
_timebar_params = {
    'timebar_interval': timedelta(minutes = 5)
}
_timebarman = TimebarManager(_timebar_params)

app = Dash(__name__)
app.layout = html.Div([
    html.H1('Bianance Futures乖離率のダッシュボード的なもの'),
    dcc.Graph(id='live-update-graph'),
    dcc.Interval(
        id='interval-component',
        interval=60*1000, # in milliseconds
        n_intervals=0)])

@app.callback(Output('live-update-graph', 'figure'),
            Input('interval-component', 'n_intervals'))
def update_graph_live(n):
    _now_timestamp = (datetime.now(tz = timezone.utc) - timedelta(minutes = 5)).timestamp()
    _timebar_idx: int = int(_now_timestamp // timedelta(minutes = 5).total_seconds() * timedelta(minutes = 5).total_seconds())
    _to_datetime = datetime.fromtimestamp(_timebar_idx, timezone.utc)
    _from_datetime = _to_datetime - timedelta(minutes = 5) * 5
    _timebar_table_name = TimebarManager.get_table_name()

    _df_timebar = TimescaleDBManager.read_sql_query(f'SELECT * FROM "{_timebar_table_name}" WHERE datetime BETWEEN \'{_from_datetime}\' AND \'{_to_datetime}\' ORDER BY datetime DESC, symbol ASC', 'TimebarManager')
    _df_timebar['deviation'] = (_df_timebar['close'] - _df_timebar['mark_close']) / _df_timebar['mark_close'] * 100
    _df_deviation = _df_timebar.pivot(index = 'datetime', columns = 'symbol', values='deviation')
    _df_deviation.drop('BTCSTUSDT', axis = 1, inplace = True)

    _rows = 16
    _columns = len(_df_deviation.columns) // 16 + 1
    _fig = make_subplots(rows = _rows, cols = _columns, shared_xaxes = True, shared_yaxes = True, subplot_titles = ['temp' for i in range(len(_df_deviation.columns))])

    for i in range(0, len(_df_deviation.columns)):
        _series = _df_deviation.iloc[:, i]
        _column_name = _df_deviation.columns[i]
        _fig.add_trace(go.Scatter(x = _series.index, y = _series.values, name = _column_name), col = i % _columns + 1, row = i // _columns + 1)
        _fig.layout.annotations[i]['text'] = _column_name
    _fig.update_layout(width = 1800, height = 1200, showlegend = False)
    _fig.update_xaxes(showticklabels = False)
    
    return _fig

if __name__ == '__main__':
    app.run_server(debug=True, host = '0.0.0.0', port = '8050')