package filelogs


//"../tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"

type Storage struct {
	storageDirPath string
}

func New(storageDirPath string) (*Storage, error) {
	return &Storage{storageDirPath: storageDirPath}, nil
}

func (s *Storage) Stop() error {
	return nil
}

