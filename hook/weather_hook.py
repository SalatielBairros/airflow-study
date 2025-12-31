import requests
from airflow.providers.http.hooks.http import HttpHook

class WeatherHook(HttpHook):
    """
    WeatherHook (HttpHook) — comportamento de conexões no Airflow 3.x

    Notas importantes:
    - A partir do Airflow 3.x o código de tarefas NÃO PODE mais acessar diretamente
        o metadata database. Isso faz parte da mudança "Restricted Metadata Database
        Access" (veja release notes do Airflow 3.x).

    - Consequência prática: chamadas para carregar conexões do DB (por exemplo,
        através de `get_conn()` ou chamadas que acessam `Connection.get()` no
        momento de import/instânciação) podem lançar
        `AirflowNotFoundException: The conn_id `...` isn't defined` quando o
        código estiver rodando fora do "execution context" (ex.: rodando um
        script Python diretamente, ou em contextos do Task SDK que não
        exponham o contexto de execução).

    - O comportamento que você observou (aparecer em `airflow connections list`
        mas falhar em `get`/`test`) é consequência desse isolamento/contexto.

    O que fazer para desenvolvimento / testes locais
    - Para testar código localmente (fora do contexto de execução do Airflow)
        use variáveis de ambiente no formato `AIRFLOW_CONN_<CONN_ID_MAIUSCULO>`.
        Exemplo:

            export AIRFLOW_CONN_TWITTER_DEFAULT='http://labdados.com'

        O Airflow carrega automaticamente conexões a partir dessas variáveis,
        independentemente de haver ou não acesso direto ao metadata DB.

    - Em produção (executando via DAG/Task) deixe a conexão persistida no
        backend de metadados ou exponha-a via secrets backend / REST API, conforme
        a sua arquitetura.

    Por que isso foi mudado
    - Mudança arquitetural (Task SDK / Execution API) para isolar execução de
        tarefas do servidor e do banco de metadados; tarefas passam a depender de
        um contrato (API/Context) em vez de consultas diretas ao DB.

    Referências:
    - Airflow 3.0 release notes: https://airflow.apache.org/docs/apache-airflow/3.0.0/release_notes.html
    - Seções: "Restricted Metadata Database Access" e "Task SDK / Task Execution API"

    Resumo prático:
    - Não chame `get_conn()` no momento da importação/instânciação se você
        pretende executar o script fora do contexto do Airflow; prefira usar
        variáveis de ambiente para testes locais ou garantir que o runtime
        (DAG/task) exponha o contexto apropriado.
    """

    def __init__(self, start_date: str, end_date: str, city: str, conn_id=None):
        self.conn_id = conn_id or 'VisualCrossingWeatherAPI'
        self.start_date = start_date
        self.end_date = end_date
        self.city = city
        super().__init__(http_conn_id=self.conn_id, method='GET')

    def create_url(self):
        conn = self.get_connection(self.http_conn_id)
        key = conn.extra_dejson.get("key")
        return f'{self.base_url}{self.city}/{self.start_date}/{self.end_date}?unitGroup=metric&include=days&key={key}&contentType=json'
    
    def connect_to_endpoint(self, url: str, session: requests.Session):
        request = requests.Request('GET', url)
        prep = session.prepare_request(request)
        self.log.info("Connecting to Visual Crossing Weather API at %s", url)
        response = self.run_and_check(session, prep, {})        
        return [response.json()]
    
    def run(self):
        session = self.get_conn()
        url_raw = self.create_url()

        return self.connect_to_endpoint(url_raw, session)