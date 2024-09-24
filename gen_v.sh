dp-cli gen --connstr "postgresql://things:things2024@localhost:5432/thingsdb?sslmode=disable" \
--tables=v_device_with_points --model_naming "{{ toUpperCamelCase ( replace . \"v_\" \"\") }}"  \
--file_naming "{{ toLowerCamelCase ( replace . \"v_\" \"\") }}" \
--module access-service

