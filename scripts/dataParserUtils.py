# events to be filtered
TELEMETRY_FILTER_LIST = ["LOGCAREPACKAGELAND",
                        "LOGCAREPACKAGESPAWN",
                        "LOGITEMATTACH",
                        "LOGITEMDETACH",
                        "LOGITEMDROP",
                        "LOGITEMEQUIP",
                        "LOGITEMPICKUP",
                        "LOGITEMPICKUPFROMCAREPACKAGE",
                        "LOGITEMPICKUPFROMLOOTBOX",
                        "LOGITEMUNEQUIP",
                        "LOGPARACHUTELANDING",
                        "LOGPLAYERCREATE",
                        "LOGPLAYERLOGIN",
                        "LOGPLAYERLOGOUT",
                        "LOGREDZONEENDED",
                        "LOGSWIMEND",
                        "LOGSWIMSTART",
                        "LOGVAULTSTART",
                        "LOGVEHICLELEAVE",
                        "LOGVEHICLERIDE",
                        ]
# possibly useful events
TELEMETRY_EVENTS_LIST = ["LOGARMORDESTROY",
                        "LOGGAMESTATEPERIODIC",
                        "LOGHEAL",
                        "LOGITEMUSE",
                        "LOGMATCHDEFINITION",
                        "LOGMATCHEND",
                        "LOGMATCHSTART",
                        "LOGOBJECTDESTROY",
                        "LOGPLAYERATTACK",  # player attack event
                        "LOGPLAYERKILL", # kill evnet
                        "LOGPLAYERMAKEGROGGY", # player down event
                        "LOGPLAYERPOSITION", # position log
                        "LOGPLAYERREVIVE", #revive log
                        "LOGPLAYERTAKEDAMAGE", # take damage log
                        "LOGVEHICLEDESTROY",
                        "LOGWEAPONFIRECOUNT",
                        "LOGWHEELDESTROY"
                        ]

def filterTelemetryEvents(telemetryEvents):
    return [e for e in telemetryEvents if e["_T"].upper() not in TELEMETRY_FILTER_LIST]
