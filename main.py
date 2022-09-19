from anime import ETLAnime


def etl_ops():
    etl = ETLAnime()

    etl.log("ETL Job Started")
    etl.extract()
    etl.transform()
    etl.load()
    etl.log("ETL Job Ended")


if __name__ == '__main__':
    etl_ops()
