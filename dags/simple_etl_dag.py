from airflow.decorators import task, dag


@task()
def placeholder():
    pass


@dag()
def simple_etl():
    placeholder()


simple_etl()
