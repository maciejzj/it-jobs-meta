# IT Jobs Meta deployment

## Prerequisites

Terraform and Ansible are required to run the deployment process.

## Infrastructure

The application is deployed through AWS. The infrastructure state is stored in a
separate S3 bucket.

To gain permissions to deploy the app to AWS use access keys. This can be done
via environmental variables:

```
 export AWS_ACCESS_KEY_ID=<AWS_ACCESS_KEY_ID>
 export AWS_SECRET_ACCESS_KEY=<AWS_SECRET_ACCESS_KEY>
```

> ❗️ **Warning:** When running the command above place space before typing it to
> prevent your credential from being stored in the shell history (this behavior
> is default in bash, can be enabled in zsh).

> ❗️ **Warning:** The state is stored on S3 without remote locking mechanism. This
> is fine for small project, but beware manipulating infrastructure resources
> from multiple devices at once.

> ⚠️ **Important:** Don't keep the keys in the open. It is most secure to destroy
> them after successful deployment.

Terraform workspaces are used for development setup. Arbitrary workspace name
can be used to setup a new environment. Infrastructure resources will be named
and tagged according to the workspace name and the service will be deployed to a
`<worksapce_name>.itjobsmeta.net` domain (e.g. for the `dev` workspace will be
deployed under `dev.itjobsmeta.net`). The main (`default`) workspace is the
deployment one—it will be exposed to `itjobsmeta.net`.

Run `terraform apply` to set up the infrastructure.

## Setup DNS

DNS has to be set up in order for the next stage of the deployment to work. The
DNS setup is not automated. You have to create `A` a record for the domain (both
`www.itjobsmeta.net` and `itjobsmeta.net`). IP address of the EC2 instance is
provided by the terraform output (`terraform output`). If you use non-default
workspace you have to use appropriate subdomain (e.g. for `dev` it should be
`www.dev.itjobsmeta.net` and `dev.itjobsmeta.net`).

## Deploy to an instance

Application deployment to EC2 is set up via Ansible. Terraform automatically
sets up Ansible inventory for the deployment.

The itjobsmeta applications are run as systemd services. App version (git commit
or tag) and launch parameters are specified in `it_jobs_meta_vars.yml` file. The
CLI command parameters specified in the file are appended to the main commands.
Read the main README or use `--help` option to see the available options (e.g.
enable/disable pipeline scheduling, archive mode, data lake archival, etc.).

Run `ansible-playbook playbook.yml` to deploy the app to the instance (wait a
few seconds afterwards before visiting the website). Deployed app should become
available under the specified domain shortly.

> 📝 **Notice:** The deployment playbook sets up cache for EC2. When the
> dashboard server is running the EC2 T2 Micro instance has sufficient memory
> limit. However, when it is to run the dashboard and the pipeline
> simultaneously RAM becomes scarce. Swap helps to mitigate it, however it uses
> EBS disk space which can generate costs. If the pipeline is run sparsely and
> swappiness is low, the EBS costs should be negligible.

## Manual access

The EC2 instance can be directly accessed via ssh using the generated `.pem`
file in the artifacts directory. Run:
`ssh -i artifacts/it-jobs-meta-ec2-server.pem ubuntu@<EC2_INSTANCE_IP>`
to log into the server (use the IP address from Terraform output).
