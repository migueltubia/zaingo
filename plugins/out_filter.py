from core import globals
import logging
import re


class OutFilter:

    FILTERS = []

    FILTERS_COLLECTION = "out_filters_collection"

    id = ""
    name = ""

    rules = []
    outs = {}

    def __init__(self):
        self.logger = logging.getLogger('Output Filters')
        self.id = ""
        self.name = ""
        self.rules = []
        self.outs = {}

    def validate_filters(self, alert):
        result = True
        for r in self.rules:
            result = result and r.calculate(alert)
        return result

    def decode_json(self, data):
        try:
            self.id = data["_id"]
            self.name = data["name"]
            rules = data["rules"]
            for r in rules:
                c = Condition()
                c.field = r["field"]
                c.operation = r["operation"]
                c.value = r["value"]
                self.rules.append(c)
            outs = data["outs"]
            for k, v in outs.items():
                o = []
                for p in v:
                    o.append(p)
                self.outs[k] = o
        except Exception as error:
            self.logger.error("Error decoding data: %s", error)


    @staticmethod
    def search_filter_by_alert(alert):
        alert_filters = []

        filters = globals.db.SearchDocuments(OutFilter.FILTERS_COLLECTION)

        for fdb in filters:
            f = OutFilter()
            f.decode_json(fdb)

            validation = f.validate_filters(alert)
            if validation:
                alert_filters.append(f)

        return alert_filters


class Condition(object):
    field = ""
    operation = ""
    value = None

    def __init__(self):
        self.logger = logging.getLogger('Out Filter Condition')

    def calculate(self, alert):
        self.logger.debug("Calculating condition %s, with operation %s", alert, self)
        result = None

        f = Functions().get_function(self.operation)
        self.logger.debug("function loaded is %s", f)
        field_value = None
        try:
            field_value = getattr(alert, self.field)
        except Exception as error:
            self.logger.error("Error getting field from data: %s", error)
        if not field_value is None:
            self.logger.debug("Field loaded is %s", field_value)
            result = f(self.value, field_value)
        else:
            result = False

        self.logger.debug("Condition is %r", result)
        return result

    def __str__(self):
        return '{field} {ope} {value}'.format(field=self.field, ope=self.operation, value=self.value)


class Functions(object):
    def __init__(self):
        self.logger = logging.getLogger('OutFilters Functions')
        pass

    def equals(self, var1, var2):
        result = False
        if var1 == var2:
            result = True
        return result

    def regex(self, var1, var2):
        result = False
        if not re.search(var2, var1) is None:
            result = True
        return result

    def like(self, text, pattern):
        self.logger.debug("Like function with text %s and pattern %s", text, pattern)
        if not (isinstance(text, str) and isinstance(pattern, str)):
            self.logger.debug("Variables are not strings")
            return False
        # If we reach at the end of both strings, we are done
        if len(text) == 0 and len(pattern) == 0:
            self.logger.debug("Var len is zero")
            return True

        # Make sure that the characters after '*' are present
        # in second string. This function assumes that the first
        # string will not contain two consecutive '*'
        if len(text) > 1 and text[0] == '*' and len(pattern) == 0:
            self.logger.debug("pattern len is zeo")
            return False

        # If the first string contains '?', or current characters
        # of both strings match
        if (len(text) > 1 and text[0] == '?') or (len(text) != 0
                                                  and len(pattern) != 0 and text[0] == pattern[0]):
            return self.like(text[1:], pattern[1:])

        # If there is *, then there are two possibilities
        # a) We consider current character of second string
        # b) We ignore current character of second string.
        if len(text) != 0 and text[0] == '*':
            return self.like(text[1:], pattern) or self.like(text, pattern[1:])

        return False

    def greater(self, var1, var2):
        if not ((isinstance(var1, int) or var1.isdigit()) and (isinstance(var2, int) or var2.isdigit())):
            self.logger.debug("Operators are not digit")
            return False

        if var2 > var1:
            return True
        else:
            return False

    def lower(self, var1, var2):
        if not ((isinstance(var1, int) or var1.isdigit()) and (isinstance(var2, int) or var2.isdigit())):
            self.logger.debug("Operators are not digit")
            return False

        if var2 < var1:
            return True
        else:
            return False

    def get_function(self, function):
        func = None
        try:
            if function == "==":
                func = getattr(self, "equals")
            elif function == "regex":
                func = getattr(self, "regex")
            elif function == "like":
                func = getattr(self, "like")
            elif function == ">":
                func = getattr(self, "greater")
            elif function == "<":
                func = getattr(self, "lower")
        except Exception as error:
            self.logger.error("Error getting function %s: %s", function, error)
        return func