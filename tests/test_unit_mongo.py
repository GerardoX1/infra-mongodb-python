
def test_mongo_repository(mocker):
    mock_address_integrator = mocker.patch(
        "infra.mongo.MongoRepository"
    )

    mock_address_integrator.__init_client_and_database.return_value = 1
    assert mock_address_integrator.__init_client_and_database() == 1
