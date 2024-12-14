import pandas as pd
from geopy.distance import geodesic
import argparse

def match_accidents_with_stations(kaggle_file: str, stations_file: str, output_file: str):
    accidents_df = pd.read_csv(kaggle_file)
    stations_df = pd.read_csv(stations_file)

    def find_nearest_station(accident):
        accident_coords = (accident["Latitude"], accident["Longitude"])
        stations_df["Distance"] = stations_df.apply(
            lambda station: geodesic(
                accident_coords, 
                (station["station_latitude"], station["station_longitude"])
            ).kilometers,
            axis=1,
        )
        nearest_station = stations_df.loc[stations_df["Distance"].idxmin()]
        return pd.Series({
            "Nearest_Station_ID": nearest_station["src_id"],
            "Station_File_Name": nearest_station["station_file_name"],
            "Distance": nearest_station["Distance"]
        })

    matched_data = accidents_df.apply(find_nearest_station, axis=1)
    result_df = pd.concat([accidents_df, matched_data], axis=1)
    result_df.to_csv(output_file, index=False)
    print(f"Accidents matched with stations saved to {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Match accidents with the nearest stations.")
    parser.add_argument("kaggle_file", type=str, help="Path to the Kaggle accidents CSV file.")
    parser.add_argument("stations_file", type=str, help="Path to the stations CSV file.")
    parser.add_argument("output_file", type=str, help="Path to save the output CSV file.")

    args = parser.parse_args()

    match_accidents_with_stations(args.kaggle_file, args.stations_file, args.output_file)

if __name__ == "__main__":
    main()
