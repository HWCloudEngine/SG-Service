class TaskApiMixin(object):

    def query_task(self, task, timeout=10):
        """ Query a task status.
        returns
            { "code": an integer,
                constants.TASK_DOING ==> Task started, but not completed
                constants.TASK_SUCCESS ==> Task complete, and successfully
                constants.TASK_ERROR ==> Task complete, but error
              "message": "the error message"
             }
        """
        params = {'t': timeout}
        if type(task) is dict:
            task = task.get('task_id', task.get('id'))
        url = self._url("/tasks/%s" % task)
        task_query =  self._result(self._get(url, params=params), True)
        return task_query

