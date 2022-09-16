import os.path
import src.crawler as cr


def main(query=None):

    data_ = cr.Crawler()
    data_.save_sql()
    data_.save_json()

main()
