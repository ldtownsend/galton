from galton.data_collection.openmeteo import get_multi_model_forecast
from galton.data_collection.city_reference import cities

if __name__ == "__main__":
    get_multi_model_forecast(cities)
