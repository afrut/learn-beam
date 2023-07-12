docker run -it `
    -w /beam `
    --mount type=bind,src="$(pwd)/beam",target=/beam `
    beam-env-java