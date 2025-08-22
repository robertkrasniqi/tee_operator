add_library(tee_library OBJECT tee_extension.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:tee_library>
        PARENT_SCOPE)