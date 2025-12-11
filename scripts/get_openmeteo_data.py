from galton.data.collection.openmeteo import get_multi_model_forecast
from galton.data.city_reference import cities

if __name__ == "__main__":
    get_multi_model_forecast(cities)
