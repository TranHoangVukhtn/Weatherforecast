import requests
from django.shortcuts import render
from .models import City
from .forms import CityForm
import psycopg2
# Thông tin kết nối cơ sở dữ liệu
db_params = {
    'dbname': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
    'host': '172.20.32.1',
    'port': '5432'
}


def index1(request):
    url = 'http://api.openweathermap.org/data/2.5/weather?q={}&units=metric&appid=c56398aa9dd4ea5e0854302e39acf5a5'
    city = 'London'

    if request.method == 'POST':
        form = CityForm(request.POST)
        form.save()

    form = CityForm()

    cities = City.objects.all()

    weather_data = []

    for city in cities:

        r = requests.get(url.format(city)).json()
        print(r)
        city_weather = {
            'city': city.name,
            'temperature': r['main']['feels_like'],
            'description': r['weather'][0]['description'],
            'icon': r['weather'][0]['icon'],
        }
        print(city_weather)
        weather_data.append(city_weather)

    context = {'weather_data': weather_data, 'form': form}
    return render(request, 'weather/weather.html', context)



def index(request):
    api_key = '3fff49a0fde94b6db4540220231610'
    alerts = 'yes'
    aqi = 'yes'
    lang = 'en'
    name = 'ho chi minh'
    url = f'http://api.weatherapi.com/v1/current.json?key={api_key}&q={name}&alerts={alerts}&aqi={aqi}&tides=yes&lang={lang}'

    response = requests.get(url)
    print(response)

    # url = 'http://api.openweathermap.org/data/2.5/weather?q={}&units=metric&appid=c56398aa9dd4ea5e0854302e39acf5a5'
    city = 'London'

    if request.method == 'POST':
        form = CityForm(request.POST)
        form.save()

    form = CityForm()

    cities = City.objects.all()

    weather_data = []

    for city in cities:

        r = requests.get( url = f'http://api.weatherapi.com/v1/current.json?key={api_key}&q={city}&alerts={alerts}&aqi={aqi}&tides=yes&lang={lang}').json()
        print(r)
        city_weather = {
            'city': city.name,
            'tz_id': r['location']['tz_id'],
            'temperature': r['location']['tz_id'],
            'localtime': r['location']['localtime'],
            'country': r['location']['country'],
            'condition': r['current']['condition']['icon'],
            # 'description': r['weather'][0]['description'],
            #  'icon': r['location']['condition']['text'],
        }
        print(city_weather)
        weather_data.append(city_weather)

    context = {'weather_data': weather_data, 'form': form}
    return render(request, 'weather/weather.html', context)



def index12345(request):
    api_key = '3fff49a0fde94b6db4540220231610'
    alerts = 'yes'
    aqi = 'yes'
    lang = 'en'
    name = 'ho chi minh'
    url = f'http://api.weatherapi.com/v1/current.json?key={api_key}&q={name}&alerts={alerts}&aqi={aqi}&tides=yes&lang={lang}'

    response = requests.get(url)
    print(response)

    # url = 'http://api.openweathermap.org/data/2.5/weather?q={}&units=metric&appid=c56398aa9dd4ea5e0854302e39acf5a5'
    city = 'London'

    if request.method == 'POST':
        form = CityForm(request.POST)
        form.save()

    form = CityForm()

    cities = City.objects.all()

    weather_data = []
    connection = psycopg2.connect(**db_params)



    for city in cities:
        r = requests.get(
            url=f'http://api.weatherapi.com/v1/current.json?key={api_key}&q={city}&alerts={alerts}&aqi={aqi}&tides=yes&lang={lang}').json()
        print(r)

        # Tạo một đối tượng cursor để thực thi các truy vấn SQL
        cursor = connection.cursor()
        # cursor.execute("SELECT * FROM df_astro where moon_illumination = '58' and name = {city.name}")
        #cursor.execute("SELECT * FROM df_astro where moon_illumination = '58' and name = 'Ho Chi Minh City'")

        cursor.execute("SELECT * FROM df_hour limit 3")
        # Lấy tất cả dòng dữ liệu kết quả
        rows = cursor.fetchall()

        # In kết quả
        for row in rows:
            print(row)
        print(rows[1][8])

        connection.commit()

        city_weather = {
            'city': city.name,
            'tz_id': rows[1][8],
            'temperature': r['location']['tz_id'],
            'localtime': r['location']['localtime'],
            'country': r['location']['country'],
            'condition': r['current']['condition']['icon'],
            # 'description': r['weather'][0]['description'],
            #  'icon': r['location']['condition']['text'],
        }
        print(city_weather)
        weather_data.append(city_weather)

    context = {'weather_data': weather_data, 'form': form}
    # return render(request, 'weather/weather.html', context)


    return render(request, 'weather/weather.html', context)

