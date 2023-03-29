#!/bin/bash
python -m apache_beam.runners.portability.expansion_service_main -p 4000 --fully_qualified_name_glob "*"