# SkyLens Backend Testing Report

## Test Cases Run
A comprehensive suite of Pytest cases was created to validate the core functionality of the FastAPI backend (`backend/main.py`), ensuring that AI query processing, database interactions, and endpoints behave safely and as expected:
1. `test_query_valid`: Verifies that a valid natural language question ("how many flights from india") correctly converts to SQL and returns rows.
2. `test_query_weather`: Verifies that weather queries execute successfully with fallback resilience.
3. `test_summary_airport`: Ensures the `/summary/{airport}` endpoint returns a complete status (weather, flights, AI summary) for a given IATA code.
4. `test_query_drop_table`: Validates security by ensuring unsafe queries (like "drop flights table") are intercepted and rejected, returning a `400 Bad Request`.
5. `test_flights_map_country_filter_bug`: Introduced to specifically target and test an edge-case bug identified during analysis regarding null values in database columns.

## Bug Found by AI Agent
While analyzing the `GET /flights-map` endpoint for weaknesses, a critical vulnerability was identified in the new "Advanced Filters" functionality:
```python
if country and flight.get("origin_country", "").lower() != country.lower():
```
**The Flaw**: Supabase API responses for rows where `origin_country` is `NULL` in PostgreSQL will evaluate to `None` in Python. `flight.get("origin_country", "")` returns `None` because the dictionary key *exists* but the value is `None`. 
Attempting to call `.lower()` on `None` throws an `AttributeError: 'NoneType' object has no attribute 'lower'`, which would crash the entire filtering loop and cause the endpoint to fail (returning an empty array to the frontend).

## Fix Applied
I updated the filtering logic in `main.py` to safely handle `None` values using Python's `or` fallback mechanism before applying `.lower()`:
```python
if country:
    origin = flight.get("origin_country") or ""
    if origin.lower() != country.lower():
        continue
```

## Final Test Result
After injecting a mock `None` value into the pytest suite and verifying the crash, the fix was applied. 
A subsequent run of `pytest` yielded a **100% pass rate** for all 5 tests:
```text
tests/test_api.py::test_query_valid PASSED                               [ 20%]
tests/test_api.py::test_query_weather PASSED                             [ 40%]
tests/test_api.py::test_summary_airport PASSED                           [ 60%]
tests/test_api.py::test_query_drop_table PASSED                          [ 80%]
tests/test_api.py::test_flights_map_country_filter_bug PASSED            [100%]

======================== 5 passed, 1 warning in 6.65s =========================
```
The backend API is now secure, extensively tested, and robust against sparse datasets.
