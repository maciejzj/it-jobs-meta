resource "aws_default_vpc" "default_vpc" {
  tags = {
    Name        = "Default AWS VPC"
    Environment = "${terraform.workspace}"
  }
}


# Create VPC
resource "aws_vpc" "my_vpc" {
  cidr_block = "10.0.0.0/24"
}

# Create internet gateway for VPC
resource "aws_internet_gateway" "gw" {
  vpc_id = aws_vpc.my_vpc.id
}

# Create public subnet
resource "aws_subnet" "public_subnet" {
  vpc_id     = aws_vpc.my_vpc.id
  cidr_block = "10.0.0.16/28"
}

# Create route table for public subnet
resource "aws_route_table" "public_rt" {
  vpc_id = aws_vpc.my_vpc.id
}

# Create route to internet gateway for public subnet
resource "aws_route" "public_rt_internet" {
  route_table_id         = aws_route_table.public_rt.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.gw.id
}

# Associate public subnet with route table
resource "aws_route_table_association" "public_subnet_association" {
  subnet_id      = aws_subnet.public_subnet.id
  route_table_id = aws_route_table.public_rt.id
}

resource "aws_security_group" "allow_web" {
  name        = "allow-web-traffic-it-jobs-meta-server-${terraform.workspace}"
  description = "Allow web trafic for hosting a server"

  vpc_id = aws_vpc.my_vpc.id

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
    Environment = "${terraform.workspace}"
  }
}
