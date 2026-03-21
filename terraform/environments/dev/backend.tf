###############################################################################
# Backend Configuration - Dev Environment
###############################################################################

terraform {
  backend "s3" {
    bucket         = "zomato-terraform-state"
    key            = "dev/terraform.tfstate"
    region         = "ap-south-1"
    dynamodb_table = "zomato-terraform-locks"
    encrypt        = true
  }
}
