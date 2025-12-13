import requests
from datetime import datetime, timedelta
import json
import os
from airflow.providers.http.hooks.http import HttpHook

class TwitterHook(HttpHook):
    """
    TwitterHook (HttpHook) — comportamento de conexões no Airflow 3.x

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

    def __init__(self, start_time: str, end_time: str, query: str, conn_id=None):
        self.conn_id = conn_id or 'twitter_default'
        self.start_time = start_time
        self.end_time = end_time
        self.query = query
        super().__init__(http_conn_id=self.conn_id, method='GET')

    def create_url(self):
        tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

        return f"{self.base_url}/2/tweets/search/recent?query={self.query}&{tweet_fields}&{user_fields}&start_time={self.start_time}&end_time={self.end_time}"
    
    def connect_to_endpoint(self, url: str, session: requests.Session):
        request = requests.Request('GET', url)
        prep = session.prepare_request(request)
        self.log.info("Connecting to Twitter API at %s", url)
        return self.run_and_check(session, prep, {})
    
    def paginate(self, url_raw: str, session: requests.Session):

        lista_json_response = []
        response = self.connect_to_endpoint(url_raw, session)
        json_response = response.json()
        lista_json_response.append(json_response)

        contador = 1
        while "next_token" in json_response.get("meta",{}) and contador < 100:
            next_token = json_response['meta']['next_token']
            url = f"{url_raw}&next_token={next_token}"
            response = self.connect_to_endpoint(url, session)
            json_response = response.json()
            lista_json_response.append(json_response)
            contador += 1

        return lista_json_response
    
    def run(self):
        session = self.get_conn()
        url_raw = self.create_url()

        return self.paginate(url_raw, session)


if __name__ == "__main__":
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() +      timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)   
    query = "datascience"
    # Para facilitar testes locais, definimos a variável de ambiente
    # AIRFLOW_CONN_TWITTER_DEFAULT aqui antes de instanciar a hook.
    # Em produção, prefira configurar essa variável no seu ambiente
    # (por exemplo no .zshrc) ou usar secrets backend do Airflow.
    os.environ.setdefault('AIRFLOW_CONN_TWITTER_DEFAULT', 'http://labdados.com')
    TwitterHook(end_time, start_time, query).run()

    for pg in TwitterHook(end_time, start_time, query).run():
        print(json.dumps(pg, indent=4, sort_keys=True))