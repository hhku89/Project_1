"""
Provides provenance information from git.

The "setup.py" is modified so that on "build",
definitions of "log" and "status" are appended,
corresponding to the outputs of "git log -1" 
and "git status".

Usage:

    import sdw4_mapping.git_info)
    print(sdw4_mapping.git_info.log)
    print(sdw4_mapping.git_info.status)
"""

# automatically generated code should override these
log = None     
status = None
