import datetime
from ..main import BodyBase

class CorrelationBasic(BodyBase):
    def generateMessage(self, alert):
        #Datos de la alerta
        body="<strong><ins>Alerta de seguridad</ins></strong><br/><br/>"
        body+="<b>Nombre de alerta:</b> %s<br/>" % alert.name
        body+="<b>Descripción de alerta:</b> %s<br/>" % alert.description
        body+="<b>Categoría de alerta:</b> %s<br/>" % alert.category
        body+="<b>Prioridad de la alerta:</b> %s<br/>" % str(alert.severity)
        body+="<b>Motor de generación:</b> %s<br/>" % alert.engine
        body+="<b>Tipo de alerta:</b> %s<br/>" % alert.alert_type
        body+="<b>Fecha de creación (UTC):</b> %s<br/><br/>" % datetime.datetime.utcfromtimestamp(alert.time).strftime("%H:%M:%S %d/%m/%Y")
        
        #Datos de la lógica
        body+="<b>Motivos de creación de la alerta</b><br/>"
        body+=""

        root_logic=alert.get_root_logic()
        for logic in root_logic:
            #body+="<i>Primera regla</i>: %s<br/>" % logic.resume
            #body+="La regla tenía la clave <b>%s</b> con valor <b>%s</b><br/>" % (str(logic.data["key"]), str(logic.data["key_value"]))
            #if logic.data["occurred"]>0:
            #    body+="Los eventos han ocurrido %s veces<br/>" % str(logic.data["occurred"])
            #body+="La regla tenía la clave de seguimiento <b>%s</b> con valores <b>%s</b><br/>" % (logic.data["follow_on"], str(logic.data["follow"]))
            body+=self.generateLogicBody(logic, 0)
            body+="<br/>"
            body+=self.generateNextLogic(logic, 1)

        #Firma del correo
        body+="<br/><br/><b>Equipo de Seguridad de External</b><br/>soporte.seguridad@grupoversia.com"
        return body

    def generateLogicBody(self, logic, order):
        body=""
        body+=("&nbsp;"*order*4) + "<i>Resumen</i>: %s<br/>" % logic.resume
        body+=("&nbsp;"*order*4) + "La regla tenía la clave <b>%s</b> con valor <b>%s</b><br/>" % (str(logic.data["key"]), str(logic.data["key_value"]))
        if "occured" in logic.data and logic.data["occured"]>0:
            body+=("&nbsp;"*order*4) + "Los eventos han ocurrido %s veces<br/>" % str(logic.data["occured"])
        if "follow_on" in logic.data:
            body+=("&nbsp;"*order*4) + "La regla tenía la clave de seguimiento <b>%s</b> con valores <b>%s</b><br/>" % (logic.data["follow_on"], str(logic.data["follow"]))
        return body

    def generateNextLogic(self, logic, order):
        body=""
        for logic_child in logic.next_logic:
            body+=self.generateLogicBody(logic_child, order)
            body+="<br/>"
            order+=1
            body+=self.generateNextLogic(logic_child, order)
        return body