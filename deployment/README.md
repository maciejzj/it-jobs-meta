# IT Jobs Meta deployment

## Infrastructure

The application is deployed through AWS. The infrastructure state is stored in a
separate S3 bucket. To setup Terraform with S3 Bucket backend run:
```sh
terraform init -backend-config="access_key=<your access key>" -backend-config="secret_key=<your secret key>"
```

> ❗️ Warning: when running the command above place space before typing it to
> prevent your credential from being stored in the shell history (this behavior
> is default in bash, can be enabled in zsh).

> ❗️ Warning: the state is stored on S3 without remote locking mechanism. This
> is fine for small project, but beware manipulating infrastructure resources
> from multiple devices at once.

## Deploy to instance

After the infrastructure is set up download the source code (either via git or
by obtaining a release download). Then run: `./deployment/deploy`.

> 📝 Notice: the `deploy` script sets up cache for EC2. When the dashboard
> server is running the EC2 T2 Micro instance has sufficient memory limit.
> However, when it is to run the dashboard and the pipeline simultaneously there
> is not enough RAM. Cache helps to mitigate it, however it uses EBS disk space
> which can generate costs. If the pipeline is run sparsely and swappiness is
> low, > the EBS costs should be negligible.
