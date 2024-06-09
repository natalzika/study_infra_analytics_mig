import boto3
import json
import os
import sys
from datetime import datetime
from botocore.exceptions import ClientError

# Cria um cliente boto3 para o Glue usando as credenciais e região do perfil padrão do AWS CLI
glue_client = boto3.client('glue')
s3_client = boto3.client('s3')

# Função para obter detalhes dos Glue Jobs
def get_glue_jobs():
    jobs = []
    response = glue_client.get_jobs()
    jobs.extend(response['Jobs'])
    
    while 'NextToken' in response:
        response = glue_client.get_jobs(NextToken=response['NextToken'])
        jobs.extend(response['Jobs'])
    
    return jobs

# Função para obter detalhes das configurações de segurança
def get_security_configuration(config_name):
    try:
        response = glue_client.get_security_configuration(Name=config_name)
        return response['SecurityConfiguration']
    except glue_client.exceptions.EntityNotFoundException:
        return None

# Função para obter detalhes das conexões
def get_connection(connection_name):
    try:
        response = glue_client.get_connection(Name=connection_name)
        return response['Connection']
    except glue_client.exceptions.EntityNotFoundException:
        return None

# Função para obter detalhes dos triggers
def get_triggers():
    triggers = []
    response = glue_client.get_triggers()
    triggers.extend(response['Triggers'])
    
    while 'NextToken' in response:
        response = glue_client.get_triggers(NextToken=response['NextToken'])
        triggers.extend(response['Triggers'])
    
    return triggers

# Função para converter objetos datetime para strings
def datetime_converter(obj):
    if isinstance(obj, datetime):
        return obj.strftime('%Y-%m-%dT%H:%M:%S')
    raise TypeError("Type not serializable")

# Função para tratar as informações dos jobs
def process_job_details(job):
    try:
        job_name = job['Name']
        job_details = {
            "name": job.get('Name', ''),
            "role_arn": job.get('Role', ''),
            "command": job.get('Command', {}),
            "max_retries": job.get('MaxRetries', 0),
            "connections": job.get('Connections', {}).get('Connections', []),
            "default_arguments": job.get('DefaultArguments', {}),
            "description": job.get('Description', ''),
            "glue_version": job.get('GlueVersion', ''),
            "max_capacity": job.get('MaxCapacity', 0),
            "timeout": job.get('Timeout', 2880),
            "worker_type": job.get('WorkerType', ''),
            "number_of_workers": job.get('NumberOfWorkers', 0),
            "security_configuration": job.get('SecurityConfiguration', ''),
        }
        
        # Adicionar detalhes de security configuration
        security_config_name = job.get('SecurityConfiguration')
        if security_config_name:
            job_details['security_configuration'] = get_security_configuration(security_config_name)
        
        return job_name, job_details
    except Exception as e:
        print(f"Error processing job details for {job['Name']}: {e}")
        return None, None

# Função para criar arquivos Terraform
def create_terraform_files():
    os.makedirs('terraform', exist_ok=True)
    os.makedirs('terraform/files', exist_ok=True)
    
    with open('terraform/main.tf', 'w') as main_file, \
         open('terraform/variables.tf', 'w') as var_file, \
         open('terraform/outputs.tf', 'w') as out_file, \
         open('terraform/terraform.tfvars', 'w') as tfvars_file:
        
        main_file.write('provider "aws" {\n  region = var.aws_region\n}\n\n')
        main_file.write('module "glue" {\n')
        main_file.write('  source = "git::https://github.com/cloudposse/terraform-aws-glue.git//modules/glue_job?ref=0.4.0"\n')
        main_file.write('  jobs = var.glue_jobs\n')
        main_file.write('  tags = var.tags\n')
        main_file.write('}\n\n')
        
        var_file.write('variable "aws_region" {\n  description = "The AWS region to create resources in"\n  type = string\n}\n\n')
        var_file.write('variable "glue_jobs" {\n  description = "Map of Glue Jobs configurations"\n  type = map(any)\n}\n\n')
        var_file.write('variable "tags" {\n  description = "Tags to be applied to all resources"\n  type = map(string)\n}\n\n')
        
        tfvars_file.write('aws_region = "us-west-2"\n\n')  # Adjust as needed
        
        jobs = get_glue_jobs()
        triggers = get_triggers()
        
        glue_jobs = {}
        
        for job in jobs:
            job_name, job_details = process_job_details(job)
            if job_name and job_details:
                glue_jobs[job_name] = job_details
            
                # Save job details to JSON file for tfvars
                with open(f'terraform/files/{job_name}.tfvars.json', 'w') as tfvars_json_file:
                    json.dump(job_details, tfvars_json_file, indent=2, default=datetime_converter)
        
        tfvars_file.write('glue_jobs = {\n')
        for job_name in glue_jobs:
            tfvars_file.write(f'  {job_name} = jsondecode(file("files/{job_name}.tfvars.json"))\n')
        tfvars_file.write('}\n\n')
        
        tfvars_file.write('tags = {\n')
        tfvars_file.write('  owner-team-email = "owner@example.com"\n')
        tfvars_file.write('  tech-team-email  = "tech@example.com"\n')
        tfvars_file.write('}\n')

# Função para fazer upload dos arquivos para o bucket S3
def upload_to_s3(bucket_uri):
    bucket_name, prefix = bucket_uri.replace("s3://", "").split("/", 1)
    
    for root, dirs, files in os.walk('terraform'):
        for file in files:
            local_path = os.path.join(root, file)
            s3_path = os.path.relpath(local_path, 'terraform')
            s3_key = os.path.join(prefix, s3_path)
            
            try:
                s3_client.head_object(Bucket=bucket_name, Key=s3_key)
                print(f"File {s3_key} already exists in bucket {bucket_name}. Skipping upload.")
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    s3_client.upload_file(local_path, bucket_name, s3_key)
                    print(f"Uploaded {s3_key} to bucket {bucket_name}.")
                else:
                    print(f"Failed to check existence of {s3_key}: {e}")

# Função principal
def main(bucket_uri):
    create_terraform_files()
    upload_to_s3(bucket_uri)
    print("Arquivos Terraform criados e enviados para o bucket S3 com sucesso!")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <s3://bucket-name/prefix>")
        sys.exit(1)
    
    bucket_uri = sys.argv[1]
    main(bucket_uri)
