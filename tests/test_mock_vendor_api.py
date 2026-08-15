from fastapi.testclient import TestClient

from mock_vendor_api import API_KEY, app

client = TestClient(app)

EXPECTED_COLUMNS = {
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
}


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "online"
    assert data["total_records"] == 6_362_620


def test_transactions_requires_api_key():
    r = client.get("/v1/transactions")
    assert r.status_code == 401


def test_transactions_rejects_bad_api_key():
    r = client.get("/v1/transactions", headers={"X-API-Key": "nope"})
    assert r.status_code == 401


def test_transactions_with_api_key():
    r = client.get(
        "/v1/transactions",
        headers={"X-API-Key": API_KEY},
        params={"offset": 0, "limit": 5},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["count"] == 5
    assert len(data["data"]) == 5
    assert set(data["data"][0].keys()) == EXPECTED_COLUMNS


def test_pagination_does_not_overlap():
    r1 = client.get(
        "/v1/transactions", headers={"X-API-Key": API_KEY}, params={"offset": 0, "limit": 3}
    )
    r2 = client.get(
        "/v1/transactions", headers={"X-API-Key": API_KEY}, params={"offset": 3, "limit": 3}
    )
    d1 = r1.json()["data"]
    d2 = r2.json()["data"]
    assert len(d1) == 3
    assert len(d2) == 3
    assert d2[0] != d1[0]
    assert d1[1] != d2[0]


def test_limit_bounded():
    r = client.get(
        "/v1/transactions", headers={"X-API-Key": API_KEY}, params={"limit": 5000}
    )
    assert r.status_code == 422


def test_negative_offset_rejected():
    r = client.get(
        "/v1/transactions", headers={"X-API-Key": API_KEY}, params={"offset": -1}
    )
    assert r.status_code == 422
