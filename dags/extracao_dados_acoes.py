from airflow.sdk import dag, task
from airflow.sdk.execution_time.macros import ds_add
from datetime import date
from pathlib import Path
import pendulum

ACOES = [
    "TOTS3.SA",
    "AAPL",
    "MSFT",
    "GOOG",
    "TSLA"
]

@task()
def get_daily_data(acao: str, ds: str = None, ds_nodash: str = None):
    import yfinance as yf
    ds = ds or date.today().isoformat()
    ds_nodash = ds_nodash or ds.replace("-", "")
    
    file_path = f'/Users/salatiel/airflow/data/stocks/extraction/{acao}/{ds_nodash}.csv'
    Path(file_path).parent.mkdir(parents=True, exist_ok=True)

    yf.Ticker(acao).history(
        interval = "1h",
        start = ds_add(ds, -1),
        end = ds,
        prepost = True
    ).to_csv(file_path)

@dag(
    schedule="0 0 * * 2-6",
    start_date = pendulum.datetime(2025, 12, 1, tz="UTC"),
    catchup=True,
    dag_display_name="Extração dados ações",
    description="Extrai informações de ações do Yahoo Finances (yfinances)",
    dag_id="extracao_dados_acoes"
)
def get_stocks_dag():
    for acao in ACOES:
        get_daily_data.override(task_id=acao)(acao)

dag = get_stocks_dag()
