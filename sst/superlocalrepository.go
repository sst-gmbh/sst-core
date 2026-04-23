// Copyright Semantic STEP Technology GmbH, Germany & DCT Co., Ltd. Tianjin, China

package sst

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"git.semanticstep.net/x/sst/bboltproto"
	"git.semanticstep.net/x/sst/bleveproto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

const defaultSuperRepoName = "default"

// SuperRepository is a container that manages multiple related Repositories.
// It provides functionality to create, retrieve, list, and delete repositories
// within a single location (local directory or remote server).
//
// A SuperRepository always contains at least one "default" repository.
// All methods are context-aware for timeout and cancellation support.
type SuperRepository interface {
	// URL returns the specific location where this SuperRepository is stored.
	// For a remote SuperRepository the returned URL uses grpc:// scheme.
	// For a local SuperRepository the returned URL uses file:// scheme.
	URL() string

	// Get retrieves an existing repository by name from the SuperRepository.
	// If name is empty, it returns the "default" repository.
	// Returns an error if the repository does not exist.
	Get(ctx context.Context, name string) (Repository, error)

	// Create creates a new repository with the given name in the SuperRepository.
	// Returns an error if a repository with the same name already exists.
	// The created repository is automatically opened and ready for use.
	Create(ctx context.Context, name string) (Repository, error)

	// Delete removes a repository by name from the SuperRepository.
	// If name is empty, it deletes the "default" repository.
	// This operation permanently removes all data in the repository.
	Delete(ctx context.Context, name string) error

	// List returns the names of all repositories in the SuperRepository.
	// The returned slice is sorted alphabetically.
	List(ctx context.Context) ([]string, error)

	// RegisterIndexHandler registers a Bleve index handler for all sub-repositories
	// in this SuperRepository. Similar to Repository.RegisterIndexHandler but applies
	// to all repositories managed by this SuperRepository.
	// Returns an error if index registration fails.
	RegisterIndexHandler(*SSTDeriveInfo) error

	// Close releases all resources, including closing all opened sub-Repositories.
	// Any error encountered while closing individual repositories is returned.
	Close() error
}

type localSuperRepository struct {
	rootDir    string
	deriveInfo *SSTDeriveInfo

	mu    sync.RWMutex
	repos map[string]Repository // opened repo in memory
	known map[string]struct{}   // repos that stored in the disk
}

// NewLocalSuperRepository creates or opens a local SuperRepository at the specified directory.
// If the directory does not exist, it will be created with permissions 0755.
// If a SuperRepository already exists at the location, it will be opened.
//
// The function performs the following:
//   - Creates the root directory if it doesn't exist
//   - Scans the directory for existing repositories
//   - Creates a "default" repository if it doesn't exist
//
// Returns an error if the directory cannot be created or accessed.
func NewLocalSuperRepository(rootDir string) (*localSuperRepository, error) {
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		return nil, fmt.Errorf("create rootDir %q: %w", rootDir, err)
	}

	s := &localSuperRepository{
		rootDir: rootDir,
		repos:   make(map[string]Repository),
		known:   make(map[string]struct{}),
	}

	// 2) scan rootDir
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		return nil, fmt.Errorf("readdir %q: %w", rootDir, err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		// only store repo names
		s.known[name] = struct{}{}
	}

	// create default repo if not exists
	if _, err = s.Create(context.Background(), defaultSuperRepoName); err != nil &&
		!strings.Contains(err.Error(), "already exists") {
		return nil, err
	}

	return s, nil
}

func (s *localSuperRepository) repoDir(name string) string {
	return filepath.Join(s.rootDir, name)
}

// URL returns the file:// URL for this local SuperRepository.
func (s *localSuperRepository) URL() string {
	return (&url.URL{Scheme: "file", Path: s.rootDir}).String()
}

func (r *localSuperRepository) RegisterIndexHandler(sd *SSTDeriveInfo) error {
	r.deriveInfo = sd
	return nil
}

func (s *localSuperRepository) Get(ctx context.Context, name string) (Repository, error) {
	if name == "" {
		name = "default"
	}

	// 1) check if stored in repos
	s.mu.RLock()
	repo, ok := s.repos[name]
	s.mu.RUnlock()
	if ok {
		if repo.Bleve() == nil {
			if s.deriveInfo != nil {
				err := repo.RegisterIndexHandler(s.deriveInfo)
				if err != nil {
					return nil, err
				}
			}
		}
		return repo, nil
	}

	// 2) open repo
	dir := s.repoDir(name)
	r, err := OpenLocalRepository(dir, "default@semanticstep.net", name)
	if err != nil {
		if errors.Is(err, ErrRepositoryDoesNotExist) || errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("repository %q does not exist: %w", name, err)
		}
		return nil, fmt.Errorf("open repo %q: %w", name, err)
	}

	if s.deriveInfo != nil {
		err = r.RegisterIndexHandler(s.deriveInfo)
		if err != nil {
			return nil, err
		}
	}
	// put into repos & known
	s.mu.Lock()
	s.repos[name] = r
	s.known[name] = struct{}{}
	s.mu.Unlock()

	return r, nil
}

func (s *localSuperRepository) Create(ctx context.Context, name string) (Repository, error) {
	if name == "" {
		return nil, fmt.Errorf("empty repository name")
	}

	s.mu.RLock()
	if _, ok := s.known[name]; ok {
		s.mu.RUnlock()
		return nil, fmt.Errorf("repository %q already exists", name)
	}
	s.mu.RUnlock()

	dir := s.repoDir(name)
	r, err := CreateLocalRepository(dir, "default@semanticstep.net", name, true)
	if err != nil {
		return nil, err
	}
	r.(*localFullRepository).sr = s

	if s.deriveInfo != nil {
		r.RegisterIndexHandler(s.deriveInfo)
	}

	s.mu.Lock()
	s.repos[name] = r
	s.known[name] = struct{}{}
	s.mu.Unlock()

	return r, nil
}

func (s *localSuperRepository) Delete(ctx context.Context, name string) error {
	if name == "" {
		name = "default"
	}

	s.mu.Lock()
	repo, opened := s.repos[name]
	if opened {
		delete(s.repos, name)
	}
	_, existed := s.known[name]
	if existed {
		delete(s.known, name)
	}
	s.mu.Unlock()

	if opened {
		if closer, ok := repo.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}

	dir := s.repoDir(name)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("remove repo dir %q: %w", dir, err)
	}

	return nil
}

func (s *localSuperRepository) List(ctx context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// union(known, repos)
	namesSet := make(map[string]struct{}, len(s.known)+len(s.repos))
	for name := range s.known {
		namesSet[name] = struct{}{}
	}
	for name := range s.repos {
		namesSet[name] = struct{}{}
	}

	names := make([]string, 0, len(namesSet))
	for name := range namesSet {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// Close releases all resources by closing all opened child repositories.
func (s *localSuperRepository) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var firstErr error
	for name, repo := range s.repos {
		if closer, ok := repo.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		delete(s.repos, name)
	}
	return firstErr
}

type repoManagerService struct {
	bboltproto.UnimplementedRepoManagerServiceServer
	super SuperRepository
}

func newRepoManagerService(super SuperRepository) *repoManagerService {
	return &repoManagerService{super: super}
}

func (s *repoManagerService) ListRepos(
	ctx context.Context,
	req *bboltproto.ListReposRequest,
) (*bboltproto.ListReposReply, error) {

	names, err := s.super.List(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list repos: %v", err)
	}
	return &bboltproto.ListReposReply{Names: names}, nil
}

func (s *repoManagerService) CreateRepo(
	ctx context.Context,
	req *bboltproto.CreateRepoRequest,
) (*bboltproto.CreateRepoReply, error) {
	GlobalLogger.Info("repoManagerService remote repository")
	name := strings.TrimSpace(req.GetName())
	if name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty repo name")
	}

	if _, err := s.super.Create(ctx, name); err != nil {
		return nil, status.Errorf(codes.Internal, "create repo %q: %v", name, err)
	}
	return &bboltproto.CreateRepoReply{Name: name}, nil
}

func (s *repoManagerService) DeleteRepo(
	ctx context.Context,
	req *bboltproto.DeleteRepoRequest,
) (*bboltproto.DeleteRepoReply, error) {

	name := strings.TrimSpace(req.GetName())
	if name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "empty repo name")
	}

	if err := s.super.Delete(ctx, name); err != nil {
		return nil, status.Errorf(codes.Internal, "delete repo %q: %v", name, err)
	}
	return &bboltproto.DeleteRepoReply{}, nil
}

type SuperRepositoryServer struct {
	*grpc.Server
	super SuperRepository
}

// NewSuperServer creates a gRPC server that exposes a SuperRepository over the network.
// It initializes a local SuperRepository at the configured directory and registers
// all necessary gRPC services for remote repository access.
//
// The function performs the following:
//   - Creates/opens a local SuperRepository at c.RepoDir
//   - Registers the Bleve index handler for all repositories
//   - Sets up gRPC services: Dataset, Ref, Commit, Index, and RepoManager
//   - Enables gRPC reflection for client discovery
//
// The returned SuperRepositoryServer wraps the gRPC server and provides methods
// for graceful shutdown. Use GracefulStopAndClose() to properly stop the server
// and release all resources.
//
// Returns an error if the SuperRepository cannot be created or if service
// registration fails.
func NewSuperServer(c *RepositoryServerConfig, opts ...grpc.ServerOption) (*SuperRepositoryServer, error) {
	// for now, all repository in the superRepository will use the same bleve deriveInfo
	super, err := NewLocalSuperRepository(c.RepoDir)
	if err != nil {
		return nil, err
	}
	err = super.RegisterIndexHandler(c.DeriveInfo)
	if err != nil {
		return nil, err
	}

	r, err := super.Get(context.TODO(), "default")
	if err != nil {
		return nil, err
	}

	// if r.Bleve() == nil {
	// 	r.RegisterIndexHandler(super.deriveInfo)
	// }

	s := newServerWithConfig(c)

	dsService := datasetServiceServer{r: r, sr: super, clientID: c.ClientID, TimeNow: time.Now}
	bboltproto.RegisterDatasetServiceServer(s, &dsService)
	log.Println("datasetService has been registered")

	refService := refServiceServer{R: r, sr: super}
	bboltproto.RegisterRefServiceServer(s, &refService)
	log.Println("refService has been registered")

	commitService := commitServiceServer{r: r, sr: super}
	bboltproto.RegisterCommitServiceServer(s, &commitService)
	log.Println("commitService has been registered")

	bleveproto.RegisterIndexServiceServer(s, initIndexServiceServer(r, super))
	log.Println("IndexService has been registered")

	bboltproto.RegisterRepoManagerServiceServer(s, newRepoManagerService(super))
	log.Println("RepoManagerService has been registered")

	reflection.Register(s)

	srv := &SuperRepositoryServer{
		Server: s,
		super:  super,
	}

	return srv, nil
}

// GracefulStopAndClose gracefully stops the gRPC server and closes the repository.
func (s SuperRepositoryServer) GracefulStopAndClose() error {
	log.Println("gPRC server call GracefulStopAndClose")
	s.GracefulStop()

	var err error
	for _, repo := range s.super.(*localSuperRepository).repos {
		err = repo.Close()
	}
	return err
}
