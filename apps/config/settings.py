# -*- coding: utf-8 -*-

# postgresql://{user}:{password}@{host}/{schema}
POSTGRESQL_DSN = "postgresql://dbdev1:123456@localhost/sv1"

POSTGRESQL_POOL = (1, 3, 60)  # min, max, max_inactive_connection_lifetime
