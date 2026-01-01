# Estudos no Airflow

Registro de vários cursos da Alura realizados sobre o Airflow. A versão utilizada foi a 3.1.3. Isso significa que existem várias mudanças daqui para o que os cursos propuseram.

## Problemas e configurações

### Weather

### Stocks
- A biblioteca `yfinances`não funciona bem com o airflow. Para que ela funcione, sua importação deve ser dentro da função da task, não globalmente. 
- É preciso configurar uma conexão que aponte o host para `local` e seja indicada em `conn_id` na `DAG`.