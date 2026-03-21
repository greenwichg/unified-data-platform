###############################################################################
# Backend Configuration - Staging Environment
###############################################################################

terraform {
  backend "s3" {
    bucket         = "zomato-terraform-state"
    key            = "staging/terraform.tfstate"
    region         = "ap-south-1"
    dynamodb_table = "zomato-terraform-locks"
    encrypt        = true
  }
}
