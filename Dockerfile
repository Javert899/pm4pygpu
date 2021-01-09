FROM rapidsai/rapidsai:cuda10.1-base-ubuntu18.04-py3.8

RUN apt-get update
RUN apt-get install -y gcc python3.8-dev git

#RUN pip3 install pm4py pm4pygpu Flask Flask-Cors setuptools SQLAlchemy alembic pydantic click appdirs python-dotenv cx-Freeze charset-normalizer dogpile.cache

