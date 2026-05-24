package config

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/yuin/stagparser"
	"go.uber.org/fx"
)

func ProvideWithParams[T any](constructors ...any) fx.Option {
	typ := reflect.TypeFor[T]()
	if typ.Kind() != reflect.Pointer || typ.Elem().Kind() != reflect.Struct {
		panic(fmt.Sprintf("config.ProvideWithParams called with non-struct-pointer type %s", typ.String()))
	}

	val := reflect.New(typ.Elem()).Interface().(T)

	return fx.Options(
		fx.Supply(val),
		fx.Provide(fx.Annotate(func() any { return val }, fx.ResultTags(`group:"params"`))),
		fx.Provide(constructors...),
	)
}

func readConfig(params ...any) error {
	var readValueFuncs []func()
	for _, param := range params {
		readValues, err := registerFlagsForStruct(param)
		if err != nil {
			return fmt.Errorf("failed to register flags for struct %T: %w", param, err)
		}
		readValueFuncs = append(readValueFuncs, readValues)
	}

	var configFile string
	pflag.CommandLine.StringVarP(&configFile, "config", "c", "./config.yaml", "Path to config file")

	pflag.CommandLine.Init(os.Args[0], pflag.ExitOnError)
	pflag.Parse()

	viper.BindPFlags(pflag.CommandLine)
	viper.SetConfigFile(configFile)
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	viper.AutomaticEnv()
	viper.ReadInConfig()

	for _, fn := range readValueFuncs {
		fn()
	}

	return nil
}

func registerFlagsForStruct(param any) (func(), error) {
	val := reflect.ValueOf(param)
	if val.Kind() == reflect.Pointer {
		val = val.Elem()
	}

	definitions, err := stagparser.ParseStruct(param, "config")
	if err != nil {
		return nil, fmt.Errorf("failed to parse struct tags for %T: %w", param, err)
	}

	getValueFuncs := make(map[string]func() any)
	for fieldName, defs := range definitions {
		if len(defs) == 0 {
			continue
		}

		field := val.FieldByName(fieldName)
		if !field.IsValid() {
			continue
		}

		flag, err := parseTagDefs(defs)
		if err != nil {
			return nil, fmt.Errorf("failed to parse tag definition for field %s on %T: %w", fieldName, param, err)
		}

		switch field.Type() {
		case reflect.TypeFor[time.Duration]():
			defVal, _ := time.ParseDuration(flag.DefValue)
			pflag.DurationP(flag.Name, flag.Shorthand, defVal, flag.Usage)
			getValueFuncs[fieldName] = func() any { return viper.GetDuration(flag.Name) }

			continue
		case reflect.TypeFor[[]string]():
			var defVal []string
			if flag.DefValue != "" {
				if err := json.Unmarshal([]byte(flag.DefValue), &defVal); err != nil {
					return nil, fmt.Errorf("failed to parse default value for string slice flag %s: %w", flag.Name, err)
				}
			}

			pflag.StringSliceP(flag.Name, flag.Shorthand, defVal, flag.Usage)
			getValueFuncs[fieldName] = func() any { return viper.GetStringSlice(flag.Name) }

			continue
		}

		switch field.Kind() {
		case reflect.Bool:
			defVal, _ := strconv.ParseBool(flag.DefValue)
			pflag.BoolP(flag.Name, flag.Shorthand, defVal, flag.Usage)
			getValueFuncs[fieldName] = func() any { return viper.GetBool(flag.Name) }
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			defVal, _ := strconv.ParseInt(flag.DefValue, 10, 64)
			pflag.Int64P(flag.Name, flag.Shorthand, defVal, flag.Usage)
			getValueFuncs[fieldName] = func() any { return viper.GetInt64(flag.Name) }
		case reflect.String:
			pflag.StringP(flag.Name, flag.Shorthand, flag.DefValue, flag.Usage)
			getValueFuncs[fieldName] = func() any { return viper.GetString(flag.Name) }
		default:
			return nil, fmt.Errorf("unsupported field type %s for flag %s", field.Kind().String(), flag.Name)
		}
	}

	return func() {
		for fieldName, getValue := range getValueFuncs {
			field := val.FieldByName(fieldName)
			field.Set(reflect.ValueOf(getValue()).Convert(field.Type()))
		}
	}, nil
}

func parseTagDefs(def []stagparser.Definition) (*pflag.Flag, error) {
	defMap := make(map[string]any)
	for _, d := range def {
		defMap[d.Name()] = d.Attributes()[d.Name()]
	}

	getStringAttribute := func(attrName string) (string, error) {
		val, ok := defMap[attrName]
		if !ok {
			return "", nil
		}

		s, ok := val.(string)
		if !ok {
			return "", fmt.Errorf("attribute '%s' must be a string", attrName)
		}

		return s, nil
	}

	stringAttributeNames := []string{"name", "short", "usage"}
	stringAttributes := make(map[string]string)
	for _, attrName := range stringAttributeNames {
		val, err := getStringAttribute(attrName)
		if err != nil {
			return nil, fmt.Errorf("invalid '%s' attribute: %w", attrName, err)
		}
		stringAttributes[attrName] = val
	}

	if stringAttributes["name"] == "" {
		return nil, fmt.Errorf("missing required 'name' attribute")
	}

	var defValue string
	if defVal, ok := defMap["default"]; ok {
		if s, ok := defVal.(string); ok {
			defValue = s
		} else {
			j, err := json.Marshal(defVal)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal default value: %w", err)
			}

			defValue = string(j)
		}
	}

	return &pflag.Flag{
		Name:      stringAttributes["name"],
		Shorthand: stringAttributes["short"],
		Usage:     stringAttributes["usage"],
		DefValue:  defValue,
	}, nil
}
