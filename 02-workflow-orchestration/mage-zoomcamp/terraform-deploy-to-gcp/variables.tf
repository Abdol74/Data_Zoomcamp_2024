variable "credentials" {
  description = "my credentials to access GCP from TERRFORM"
  default     = "my-creds.json"

}

variable "project" {
  description = "my project name"
  default     = "primeval-legacy-412013"


}

variable "region" {
  description = "region of project on GCP"
  default     = "us-central1"

}

variable "google_storage_bucket_name" {
  description = "my GCP bucket name"
  default     = "mage-zoomcamp-abdol-test-bucket"

}

variable "resources_location" {
  description = "location of deployed resources on GCP"
  default     = "US"

}

variable "google_bigquery_dataset_name" {
    description = "bigquery dataset name"
    default = "google_bigquery_demo_dataset"

}