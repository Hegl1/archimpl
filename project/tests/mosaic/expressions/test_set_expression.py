from mosaic.query_executor import execute_query


def test_union_get_result():
    result, _ = execute_query("pi VorlNr as Vorgaenger voraussetzen union pi VorlNr vorlesungen;")[0]
    assert len(result.records) == 17


def test_intersect_get_result():
    result, _ = execute_query("pi VorlNr as Vorgaenger voraussetzen intersect pi VorlNr vorlesungen;")[0]
    assert len(result.records) == 7
    result, _ = execute_query("pi VorlNr as Vorgaenger voraussetzen intersect pi VorlNr as gelesenVon vorlesungen;")[0]
    assert len(result.records) == 0


def test_except_get_result():
    result, _ = execute_query("pi VorlNr as Vorgaenger voraussetzen except pi VorlNr vorlesungen;")[0]
    assert len(result.records) == 6
    result, _ = execute_query("pi VorlNr vorlesungen except pi VorlNr vorlesungen;")[0]
    assert len(result.records) == 0
