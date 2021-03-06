from utils import functions as functions
import logging

class Condition:
    field = ""
    operation = ""
    value = None
    logical_operation = ""

    conditions = []

    def __init__(self):
        self.logger = logging.getLogger('Condition')
        self.field = ""
        self.operation = ""
        self.value = None
        self.logical_operation = ""
        self.conditions = []

    @staticmethod
    def create_condition(o):
        condition = Condition()

        condition.field = o["field"] if "field" in o.keys() else ""
        condition.value = o["value"] if "value" in o.keys() else ""
        condition.operation = o["operation"] if "operation" in o.keys() else ""
        condition.logical_operation = o["logical_operation"] if "logical_operation" in o.keys() else ""

        if "conditions" in o.keys():
            for m in o["conditions"]:
                sub_cond = Condition.create_condition(m)
                condition.conditions.append(sub_cond)

        return condition

    def add_filter(self, filter):
        self.matches.append(filter)

    def calculate(self, ec1 = None, ec2 = None):
        self.logger.debug("Calculating condition %s, with operation %s", ec1, self)
        result = None
        if self.logical_operation != "":
            self.logger.debug("Logical operation is %s", self.logical_operation)
            result_temp = True
            for m in self.conditions:
                result_temp = m.calculate(ec1, ec2)
            if self.logical_operation.upper() == "AND":
                result = result_temp if result == None else result and result_temp
            elif self.logical_operation.upper() == "OR":
                result = result_temp if result == None else result or result_temp
            elif self.logical_operation.upper() == "NOT":
                result = not result_temp
        elif self.operation != "":
            self.logger.debug("No logical operation defined")
            f = functions.Functions().get_function(self.operation)
            self.logger.debug("function loaded is %s", f)
            field1_value = None
            field2_value = None
            try:
                field1_value = self.get_data1_field(ec1)
                field2_value = self.get_data2_field(ec2)
            except Exception as error:
                self.logger.error("Error getting field from data: %s", error)
            if field1_value != None and field2_value != None:
                self.logger.debug("Field loaded is %s -- %s", field1_value, field2_value)
                result = f(field1_value, field2_value)
            else:
                result = False
        else:
            self.logger.debug("Neither logical operation nor operation defined")
            result = True
        self.logger.debug("Condition is %r", result)
        return result

    def get_data1_field(self, ec = None):
        field_value = ec.get_attribute(self.field)
        return field_value

    def get_data2_field(self, ec = None):
        return self.value


    def __str__(self):
        if self.logical_operation != "":
            return self.logical_operation
        else:
            return '{field} {ope} {value}'.format(field=self.field, ope=self.operation, value=self.value)

class CorrelationCondition(Condition):

    def __init__(self):
        super(CorrelationCondition, self).__init__()
        self.logger = logging.getLogger('CorrelationCondition')

        @staticmethod
        def create_condition(o):
            condition = CorrelationCondition()

            condition.field = o["field"] if "field" in o.keys() else ""
            condition.value = o["value"] if "value" in o.keys() else ""
            condition.operation = o["operation"] if "operation" in o.keys() else ""
            condition.logical_operation = o["logical_operation"] if "logical_operation" in o.keys() else ""

            if "conditions" in o.keys():
                for m in o["conditions"]:
                    sub_cond = CorrelationCondition.create_condition(m)
                    condition.conditions.append(sub_cond)

            return condition

    def get_data1_field(self, ec):
        field_value = ec.get_attribute(self.field)
        return field_value

    def get_data2_field(self, ec):
        field_value = ec.get_attribute(self.value)
        return field_value