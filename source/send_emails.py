



if __name__ == '__main__':
    # get command line args
    args = common.handle_argparse()

    # configure logging
    logging = common.setup_logging(__file__, args.logs_dirpath)

    # load config
    config = common.read_config(args.config_filepath, logging)

    if config is None:
        logging.error('Invalid or missing config. Exiting {}...'.format(__file__))
        sys.exit(1)