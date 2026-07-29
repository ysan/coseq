#
#   Defines
#
BASEDIR		:=	../

CFLAGS		:=	-std=c++11

INCLUDES	:= \
	-I./ \
	-I$(BASEDIR)/coseq \
	-I$(BASEDIR)/coseqpp \

LIBS		:= \
	-lpthread \
	-L$(BASEDIR)/coseq -lcoseq \
	-L$(BASEDIR)/coseqpp -lcoseqpp \

#
#   Target object
#
TARGET_NAME	:=	testpp
NOT_INSTALL	:=	YES

#
#   Target type
#     (EXEC/SHARED/STATIC/OBJECT)
#
TARGET_TYPE	:=	EXEC

#
#   Compile sources
#
SRCS_CPP	:= \
	main.cpp \

#
#   Configurations
#
include $(BASEDIR)/Config.mak
