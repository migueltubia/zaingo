import logging
import re

class Functions (object):

    def __init__(self):
        self.logger = logging.getLogger('Functions')
        pass

    def equals(self, var1, var2):
        result = False
        if var1 == var2:
            result = True
        return result

    def regex(self, var1, var2):
        result = False
        if re.search(var2, var1) != None:
            result = True
        return result

    def like(self, text, pattern):
        self.logger.debug("Like function with text %s and pattern %s", text, pattern)
        if not (isinstance(text, str) and isinstance(pattern, str)):
            return False
        # If we reach at the end of both strings, we are done
        if len(text) == 0 and len(pattern) == 0:
            return True

        # Make sure that the characters after '*' are present
        # in second string. This function assumes that the first
        # string will not contain two consecutive '*'
        if len(text) > 1 and text[0] == '*' and len(pattern) == 0:
            return False

        # If the first string contains '?', or current characters
        # of both strings match
        if (len(text) > 1 and text[0] == '?') or (len(text) != 0
            and len(pattern) !=0 and text[0] == pattern[0]):
            return self.like(text[1:],pattern[1:])

        # If there is *, then there are two possibilities
        # a) We consider current character of second string
        # b) We ignore current character of second string.
        if len(text) !=0 and text[0] == '*':
            return self.like(text[1:],pattern) or self.like(text,pattern[1:])

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
        except Exception as error:
            self.logger.error("Error getting function %s: %s", function, error)
        return func
