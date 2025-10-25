"""
### 매개변수를 사용하는 간단한 DAG
이 DAG는 한 개의 문자열 타입 매개변수를 사용하여 Python 데코레이터 태스크에서 활용합니다.
"""
from airflow.decorators import dag, task
from pendulum import datetime
from airflow.models.param import Param
import random

@dag(
    start_date=datetime(2023, 4, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    params={"pet_name": Param("정의되지 않음!", type="string")},
    tags=["example"],
)
def walking_my_pet():
    @task
    def walking_your_pet(**context):
        pet_name = context["params"]["pet_name"]
        minutes = random.randint(2, 10)
        print(f"{pet_name}이(가) {minutes}분 동안 산책했습니다!")

    walking_your_pet()

walking_my_pet() 