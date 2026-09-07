package docker

import (
	"reflect"
	"testing"
)

func TestNewManager(t *testing.T) {
	type args struct {
		config WorkerConfig
	}
	tests := []struct {
		name    string
		args    args
		want    *Manager
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewManager(tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewManager() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewManager() got = %v, want %v", got, tt.want)
			}
		})
	}
}
