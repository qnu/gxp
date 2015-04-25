# 
# [1] define rule templates
# 

#
# $(call expand_parameters,a b c)
#  ==> .$(a).$(b).$(c)
#

define expand_parameters
$(if $(1),.$$($(firstword $(1)))$(call expand_parameters,$(wordlist 2,$(words $(1)),$(1))))
endef

define make_rule_single
$(target) : $(output)
$(output) : $(input)
	$(cmd)
endef

# a:=1 2
# b:=3 4 
# $(call make_rule_recursive,a b)
#  ==> $(foreach a,1 2,$(call make_rule_recursive,b))
#   ==> $(foreach a,1 2,$(foreach b,3 4,$(call make_rule_recursive)))
#    ==> $(foreach a,1 2,$(foreach b,3 4,$(eval $(call make_rule_single))))
#  
define make_rule_recursivex
$(if $(1),\
  $(foreach $(firstword $(1)),$(or $($(firstword $(1))),""),$(call make_rule_recursive,$(wordlist 2,$(words $(1)),$(1)))),\
  $(eval $(call make_rule_single)))
endef

define make_rule_recursivey
$(if $(1),\
  $(foreach $(firstword $(1)),\
            $(or $($(firstword $(1))),""),\
     $(call make_rule_recursive,$(wordlist 2,$(words $(1)),$(1)))),\
  $(eval $(call make_rule_single)))
endef

define make_rule_recursive
$(if $(1),\
  $(foreach $(firstword $(1)),\
            $($(firstword $(1))),\
     $(call make_rule_recursive,$(wordlist 2,$(words $(1)),$(1)))),\
  $(eval $(call make_rule_single)))
endef

# 
# [2] set default parameters
# 

#parameters:=$(or $(parameters),a b c)
target:=$(or $(target),gxp_pp_default_target)

$(target) : 

#ifeq ($(output),)
#expanded_parameters:=$(call expand_parameters,$(parameters))
#output=gxp_pp_default_output$(expanded_parameters)
#endif

#ifeq ($(cmd),)
#cmd=echo $(call expand_parameters,$(parameters))
#endif

#
# [3] really define rules
#

define define_rules_fun
$(if $(and $(parameters),$(cmd),$(output)),\
  $(eval $(call make_rule_recursive,$(parameters))),\
  $(warning "specify at least parameters:=..., cmd=..., and output=..."))
endef

define_rules=$(call define_rules_fun)

$(and $(parameters),$(cmd),$(output),$(define_rules))

# 
# [4] clear all variables
#
#parameters:=
#target=
#input=
#output=
#cmd=

