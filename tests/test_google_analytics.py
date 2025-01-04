import os
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)


def test_analytics_data(property_id="445872593"):
    try:
        # Print the path to verify credentials location
        creds_path = "./secrets/dag-task-d31611f32561.json"
        print(f"Using credentials from: {os.path.abspath(creds_path)}")

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path

        # Create the client
        client = BetaAnalyticsDataClient()

        # Build request
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[
                Dimension(name="city"),
                Dimension(name="country"),
                Dimension(name="date"),
            ],
            metrics=[
                Metric(name="activeUsers"),
                Metric(name="sessions"),
                Metric(name="screenPageViews"),
            ],
            date_ranges=[DateRange(start_date="2024-12-16", end_date="2024-12-30")],
        )

        # Run report
        response = client.run_report(request)
        print(response)
        print("Test Report result:")

        data = []
        for row in response.rows:
            data.append(
                {
                    "city": row.dimension_values[0].value,
                    "country": row.dimension_values[1].value,
                    "date": row.dimension_values[2].value,
                    "activeUsers": row.metric_values[0].value,
                    "sessions": row.metric_values[1].value,
                    "screenPageViews": row.metric_values[2].value,
                }
            )

        df = pd.DataFrame(data)
        print("===" * 10)
        print("Raw data")
        print(df)

        # Handle different date formats and convert to datetime
        def clean_date(date_str):
            if len(date_str) == 8:  # Format: YYYYMMDD
                return pd.to_datetime(date_str, format="%Y%m%d")
            else:  # Format: YYYY-MM-DD
                return pd.to_datetime(date_str, format="%Y-%m-%d")

        # Convert date using the custom function
        df["date"] = df["date"].apply(clean_date)

        # Fill missing values with 0 and convert to int
        df["sessions"] = df["sessions"].fillna(0).astype(int)
        df["screenPageViews"] = df["screenPageViews"].fillna(0).astype(int)
        df["activeUsers"] = df["activeUsers"].fillna(0).astype(int)

        print("===" * 10)
        print("DAU report")
        # Sort by date to match SQL query output
        df_temp = (
            df[["date", "activeUsers"]]
            .groupby("date")
            .sum()
            .reset_index()
            .sort_values("date")
        )
        df_temp["activeUsers"] = df_temp["activeUsers"].astype(int)
        df_temp = df_temp[df_temp["activeUsers"] > 0]
        print(df_temp)

    except Exception as e:
        print(f"Error type: {type(e)}")
        print(f"Error message: {str(e)}")
        print("Please verify:")
        print("1. Property ID is correct")
        print("2. Service account has GA4 access")
        print("3. Credentials file exists and is valid")


if __name__ == "__main__":
    test_analytics_data()
