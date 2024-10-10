# Basic DAG Project

- I chose Airflow for this processor due to its prevalence. The Directed Acyclic Graph (DAG) that controls the pipeline is in the `dags` folder, as that is what Airflow looks for when loading DAGs onto its UI. Helper methods are in the `plugins` directory, separated by those related to database transactions and fetching/processing of data. In a more complex project one could further break down helpers into logical directories. 
- In my DAG I fetch the data from one of two mocked sources, then automatically move on to the step consolidating interests by email. The DAG can be triggered manually, and if left untriggered and active it will run once every hour.
- In the absence of a scenario under which I would want a CSV or API, or an actual API to call, I allowed the user to choose an input source based on an Airflow parameter, the default being CSV. Based on business logic, this selection could be modified.
- To locally run the project, install the contents of `requirements.txt`. Then run `airflow standalone` and go to `localhost:8080` where you can login with the credentials provided in the terminal output. For a full tutorial on getting started with Airflow please see [the official documentation](https://airflow.apache.org/docs/apache-airflow/stable/start.html).


