import pandas as pd
import requests
from io import StringIO

def get_weather_data(s3_hook, bucket, object_key):
    location_0 = 'Granja_Santa_Catarina'
    locations = {
        location_0: {'lat': -31.16, 'lon': -54.85},
        'location_O': {'lat': -31.16, 'lon': -55.85},
        'location_S': {'lat': -31.66, 'lon': -54.85},
        'location_L': {'lat': -31.16, 'lon': -53.85},
        'location_N': {'lat': -30.66, 'lon': -54.85},
    }

    weather_data_combined = []

    # Tenta ler o arquivo existente do S3
    try:
        s3_obj = s3_hook.get_key(key=object_key, bucket_name=bucket)
        existing_data = s3_obj.get()['Body'].read().decode('utf-8')
        df_existing = pd.read_csv(StringIO(existing_data))
        existing_updated_times = set(df_existing['updated_time'].astype(str))
        print("Arquivo existente carregado do S3.")
    except Exception as e:
        df_existing = pd.DataFrame()
        existing_updated_times = set()
        print("Arquivo não encontrado no S3 ou erro ao carregar. Será criado um novo.")

    for loc_name, loc_coords in locations.items():
        print(f"Processando dados para localização: {loc_name}")
        lat, lon = loc_coords['lat'], loc_coords['lon']
        dyn_url = f"https://api.met.no/weatherapi/locationforecast/2.0/complete?lat={lat}&lon={lon}"

        headers = {
            "User-Agent": "ViniciusApp/2.0 (viniciusfrantz@gmail.com)"
        }

        response = requests.get(dyn_url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            updated = data['properties']['meta'].get('updated_at', None)
            updated_time = updated.split(':')[0]
            updated_date = updated_time.split('T')[0]
            updated_hour = updated_time.split('T')[1]

            if updated_time in existing_updated_times:
                print(f"Dados de {updated_time} já existem. Pulando {loc_name}.")
                continue

            forecasts = data['properties']['timeseries']

            for forecast in forecasts:
                time = forecast['time']
                details = forecast['data']['instant']['details']
                precipitation_amount = forecast['data'].get('next_1_hours', {}).get('details', {}).get('precipitation_amount')

                if precipitation_amount is None:
                    precipitation_amount = forecast['data'].get('next_6_hours', {}).get('details', {}).get('precipitation_amount')
                if precipitation_amount is None:
                    precipitation_amount = 0.0

                weather_data_combined.append({
                    'time': time,
                    'updated_time': updated_time,
                    'updated_date': updated_date,
                    'updated_hour': updated_hour,
                    'location': loc_name,
                    'precipitation': precipitation_amount,
                    'air_pressure(sea_level)': details.get('air_pressure_at_sea_level', None),
                    'air_temperature': details.get('air_temperature', None),
                    'cloud_area_fraction': details.get('cloud_area_fraction', None),
                    'relative_humidity': details.get('relative_humidity', None),
                    'wind_from_direction': details.get('wind_from_direction', None),
                    'wind_speed': details.get('wind_speed', None),
                })

            print(f"Dados para {loc_name} processados.")
        else:
            print(f"Erro na requisição para {loc_name}: {response.status_code}")

    if weather_data_combined:
        df_new = pd.DataFrame(weather_data_combined)

        # Concatena com os dados existentes
        df_final = pd.concat([df_existing, df_new], ignore_index=True)

        # Converte em string para enviar ao S3
        csv_buffer = StringIO()
        df_final.to_csv(csv_buffer, index=False)

        s3_hook.load_string(
            string_data=csv_buffer.getvalue(),
            key=object_key,
            bucket_name=bucket,
            replace=True
        )

        print(f"Dados novos enviados para S3 em {bucket}/{object_key}")
    else:
        print("Nenhum dado novo para enviar ao S3.")