terraform {
  backend "s3" {
    bucket  = "zomato-data-platform-terraform-state"
    key     = "dev/terraform.tfstate"
    region  = "ap-south-1"
    encrypt = true
  }
}
