
def select_all_traffic():
    return "select * from traffic_raw where statDate = '2026-01-01'"

def select_statDate_traffic(date):
    query = "select * from traffic_raw where statDate = %s"
    params = (date, )
    return query, params