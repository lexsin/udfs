#!/bin/bash

main() {
	local dirs="broker consumer publisher"
	local dir

	for dir in ${dirs}; do
		pushd ${dir}
			go build
		popd
	done
}

main "$@"

