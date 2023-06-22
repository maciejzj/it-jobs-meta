resource "aws_vpc" "it_jobs_meta" {
  cidr_block = "10.0.0.0/24"

  tags = {
    Name        = "Main VPC for it-jobs-meta services"
    Project     = "${var.project_name_tag}"
    Environment = "${terraform.workspace}"
  }
}

resource "aws_internet_gateway" "it_jobs_meta_gateway" {
  vpc_id = aws_vpc.it_jobs_meta.id

  tags = {
    Name        = "Main web gateway for it-jobs-meta services"
    Project     = "${var.project_name_tag}"
    Environment = "${terraform.workspace}"
  }
}

resource "aws_subnet" "it_jobs_meta_public" {
  vpc_id     = aws_vpc.it_jobs_meta.id
  cidr_block = "10.0.0.16/28"

  tags = {
    Name        = "Public subnet for it-jobs-meta services"
    Project     = "${var.project_name_tag}"
    Environment = "${terraform.workspace}"
  }
}

resource "aws_route_table" "it_jobs_meta_public_networking" {
  vpc_id = aws_vpc.it_jobs_meta.id

  tags = {
    Name        = "Main route table for it-jobs-meta services"
    Project     = "${var.project_name_tag}"
    Environment = "${terraform.workspace}"
  }
}

# Create route to internet gateway for public subnet
resource "aws_route" "it_jobs_meta_public_networking" {
  route_table_id         = aws_route_table.it_jobs_meta_public_networking.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.it_jobs_meta_gateway.id
}

resource "aws_route_table_association" "it_jobd_meta_public_subnet_networking" {
  subnet_id      = aws_subnet.it_jobs_meta_public.id
  route_table_id = aws_route_table.it_jobs_meta_public_networking.id
}

resource "aws_security_group" "allow_web" {
  name        = "allow-web-traffic-it-jobs-meta-server-${terraform.workspace}"
  description = "Allow web traffic for hosting web server via http and https with ssh access"

  vpc_id = aws_vpc.it_jobs_meta.id

  ingress {
    description = "HTTPS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    description = "HTTP"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    description = "HTTP"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "Allow web traffic for it-jobs-meta"
    Project     = "${var.project_name_tag}"
    Environment = "${terraform.workspace}"
  }
}
