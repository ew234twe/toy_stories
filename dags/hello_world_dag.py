from airflow.decorators import task, dag


@task()
def say_hello():
    print("Hello World")


@dag()
def hello_world():
    say_hello()


hello_world()
