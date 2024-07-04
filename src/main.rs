use std::sync::Arc;

use actix_web::middleware::Logger;
use actix_web::{get, web, App, HttpResponse, HttpServer};
use datafusion::arrow;
use datafusion::arrow::array::Float32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::prelude::*;

struct WeatherStations {
    df_ctx: Arc<SessionContext>,
}

fn process_batches(batches: &[RecordBatch]) -> String {
    let mut result = String::new();
    for batch in batches {
        for column in batch.columns() {
            match column.data_type() {
                arrow::datatypes::DataType::Float32 => {
                    let array = column.as_any().downcast_ref::<Float32Array>().unwrap();
                    for i in 0..array.len() {
                        let value = array.value(i).to_string();
                        result.push_str(&value);
                    }
                }
                _ => {
                    result.push_str("Unsupported data type.\n");
                }
            }
        }
    }
    result
}

impl WeatherStations {
    async fn from_file(csv_file: String) -> Result<WeatherStations> {
        let ctx = Arc::new(SessionContext::new());

        let schema = Arc::new(Schema::new(vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("measurement", DataType::Float32, false),
        ]));

        match ctx
            .register_csv(
                "weather_stations",
                &csv_file,
                CsvReadOptions::new()
                    .schema(&*schema)
                    .has_header(false)
                    .delimiter(b';'),
            )
            .await
        {
            Ok(_) => Ok(WeatherStations { df_ctx: ctx }),
            Err(e) => return Err(e),
        }
    }

    async fn fetch_by_city(&self, city: &String) -> Result<String> {
        let df = self
            .df_ctx
            .sql(&format!(
                "SELECT CAST(measurement AS FLOAT) FROM weather_stations \
                WHERE \
                city = '{city}'"
            ))
            .await?;

        let batches = df.collect().await.unwrap();
        let result = process_batches(&batches);
        Ok(result)
    }
}

#[get("/cities")]
async fn index() -> HttpResponse {
    HttpResponse::Ok().body(format!("Hello!"))
}

#[get("/city/{city_name}")]
async fn get_city(path: web::Path<(String,)>, data: web::Data<WeatherStations>) -> HttpResponse {
    let city_name = &path.into_inner().0;
    let result = data.fetch_by_city(city_name).await;

    HttpResponse::Ok().body(match result {
        Ok(data) => data,
        Err(e) => format!("Error: {}", e),
    })
}

#[actix_web::main]
async fn main() -> Result<(), std::io::Error> {
    let input = std::path::PathBuf::from("assets/weather_stations.csv")
        .to_str()
        .unwrap()
        .to_string();

    let weatherstations = WeatherStations::from_file(input).await?;
    let app_state = web::Data::new(weatherstations);

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(app_state.clone())
            .service(index)
            .service(get_city)
    })
        .bind(("127.0.0.1", 8080))?
        .run()
        .await
}
