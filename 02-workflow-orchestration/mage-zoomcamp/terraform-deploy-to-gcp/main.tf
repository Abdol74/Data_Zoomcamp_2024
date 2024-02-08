terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.12.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}


resource "google_storage_bucket" "gcp-bucket-instance-01" {
  name          = var.google_storage_bucket_name
  location      = var.resources_location
  force_destroy = true

}


resource "google_bigquery_dataset" "bigquery_demo_dataset" {
  dataset_id = var.google_bigquery_dataset_name
}