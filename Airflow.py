from datetime import datetime
import subprocess
import time


class Task:
    def __init__(self, task_id):
        self.task_id = task_id
        self.next_tasks = []

    def set_next(self, *tasks):
        self.next_tasks.extend(tasks)
        return tasks[-1]

    def execute(self):
        print(f"Ejecutando tarea: {self.task_id}")


class BashTask(Task):
    def __init__(self, task_id, bash_command):
        super().__init__(task_id)
        self.bash_command = bash_command

    def execute(self):
        super().execute()
        subprocess.run(self.bash_command, shell=True)


class Workflow:
    def __init__(self, name, schedule="@daily"):
        self.name = name
        self.schedule = schedule
        self.tasks = []

    def add_task(self, task):
        self.tasks.append(task)
        return task

    def run(self):
        print(f"Iniciando workflow: {self.name}")
        for task in self.tasks:
            task.execute()
            time.sleep(1)  # Pequeña pausa entre tareas
        print(f"Workflow {self.name} completado")


# Crear el workflow
workflow = Workflow('mi_primer_workflow')

# Definir las tareas
inicio = workflow.add_task(Task('inicio'))
procesamiento = workflow.add_task(BashTask('procesamiento_datos', 'echo "Procesando datos!"'))
notificacion = workflow.add_task(BashTask('envio_notificacion', 'echo "Notificación enviada!"'))
fin = workflow.add_task(Task('fin'))

# Definir el flujo de trabajo
inicio.set_next(procesamiento)
procesamiento.set_next(notificacion)
notificacion.set_next(fin)

# Ejecutar el workflow
if __name__ == "__main__":
    workflow.run()

    # Tarea paralela alternativa
    # inicio >> [procesamiento, notificacion] >> fin

# Documentación adicional
"""
Este DAG demuestra:
1. Creación de tareas simples con DummyOperator
2. Tareas que ejecutan comandos Bash con BashOperator
3. Definición de dependencias entre tareas
4. Programación diaria

Recursos útiles:
- Documentación oficial: https://airflow.apache.org
- Tutorial en video: https://www.youtube.com/watch?v=ewK4KszmeTI
- Guía en español: https://aprenderbigdata.com/apache-airflow
"""