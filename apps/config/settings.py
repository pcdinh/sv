# -*- coding: utf-8 -*-

# It is possible to configure many database servers here
DATABASE = {
    "default": {
        # postgresql://{user}:{password}@{host}/{schema}
        "dsn": "postgresql://dbdev1:123456@localhost/sv1",
        "pool": {
            "min": 1,
            "max": 3,
            "max_inactive_connection_lifetime": 60
        }
    }
}

