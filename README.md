# <img width="1158" alt="image" src="https://user-images.githubusercontent.com/102450381/161465008-46a91b70-07f1-44fa-8a40-d0f2275e8b79.png">

# Infrastructure Required:
    - PostgresQL RDS Database
    <img width="1170" alt="image" src="https://user-images.githubusercontent.com/102450381/161468106-d423f84f-72b4-471f-8781-36e345ef509b.png">

    - Redshift Cluster + Redshift Database
    <img width="346" alt="image" src="https://user-images.githubusercontent.com/102450381/161468223-1d87532b-bf80-49f2-8422-1410c38d8fba.png">

        - Can view respective table creation script / queries for these in the databases folder
    - AWS Step Function
    <img width="347" alt="image" src="https://user-images.githubusercontent.com/102450381/161468318-8427f31f-f318-42bf-9fdb-c0f38348b027.png">

    - AWS Glue Jobs
    - AWS Glue Data Catalog Database/Tables - craft-project
    - AWS Glue Crawlers
    - AWS Glue Database Connections
    - AWS s3 bucket
    - Python Query Script
   
# Other Steps
    - Create VPC S3 endpoints for Glue Jobs
        - https://aws.amazon.com/premiumsupport/knowledge-center/glue-s3-endpoint-validation-failed/
    - Ensure to configure RDS inbound rules in the VPC security groups 
        - https://dev.to/amree/exporting-data-from-rds-to-s3-using-aws-glue-mai
