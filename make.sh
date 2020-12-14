#!/bin/sh

os_archs=()

# Reference:
# https://github.com/golang/go/blob/master/src/go/build/syslist.go

package="github.com/ankit-arora/clevertap-data-upload"
package_name="clevertap-data-upload"

for goos in darwin linux windows
do
    for goarch in amd64
    do
	output_name=${package_name}-${goos}-${goarch}
    	if [ ${goos} = "windows" ]; then
        	package_name+='.exe'
    	fi
        GOOS=${goos} GOARCH=${goarch} go build -o ${goos}/${package_name} $package
        if [ $? -eq 0 ]
        then
            os_archs+=("${goos}/${goarch}")
        fi
    done
done

for os_arch in "${os_archs[@]}"
do
    echo ${os_arch}
done
