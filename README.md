# <img width="1158" alt="image" src="https://user-images.githubusercontent.com/102450381/161465008-46a91b70-07f1-44fa-8a40-d0f2275e8b79.png">

# AWS Services/Infrastructure Required:
### s3 Bucket
<img width="692" alt="image" src="https://user-images.githubusercontent.com/102450381/161468668-b5f56868-b49f-4e0c-8376-c5ebdeefb3e6.png">
dropzone Folder
<img width="715" alt="image" src="https://user-images.githubusercontent.com/102450381/161468705-6e49fd88-a5bb-4fd9-a754-6df68ddafcef.png">

### PostgresQL RDS Database
<img width="1170" alt="image" src="https://user-images.githubusercontent.com/102450381/161468106-d423f84f-72b4-471f-8781-36e345ef509b.png">

### Redshift Cluster + Redshift Database
<img width="346" alt="image" src="https://user-images.githubusercontent.com/102450381/161468223-1d87532b-bf80-49f2-8422-1410c38d8fba.png">
    
### AWS Step Function 
<img width="347" alt="image" src="https://user-images.githubusercontent.com/102450381/161468318-8427f31f-f318-42bf-9fdb-c0f38348b027.png">

### AWS Glue Jobs
<img width="1758" alt="image" src="https://user-images.githubusercontent.com/102450381/161468600-4ba1af2c-2b1f-44af-ba6a-2151bba1f369.png">

### AWS Glue Data Catalog Database/Tables
<img width="1155" alt="image" src="https://user-images.githubusercontent.com/102450381/161468768-7d898b79-9b56-4c13-9afb-9d68bf88cd6a.png">

### AWS Glue Database Connections
<img width="741" alt="image" src="https://user-images.githubusercontent.com/102450381/161468825-9f88d759-6d21-437b-b52c-199e113ce731.png">

### Python Query Script - Check the SQLQueries folder
   
# Other Steps
    - Create VPC S3 endpoints for Glue Jobs
        - https://aws.amazon.com/premiumsupport/knowledge-center/glue-s3-endpoint-validation-failed/
    - Ensure to configure RDS inbound rules in the VPC security groups 
        - https://dev.to/amree/exporting-data-from-rds-to-s3-using-aws-glue-mai
