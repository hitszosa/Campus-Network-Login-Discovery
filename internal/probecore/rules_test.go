package probecore

import "testing"

func TestValidateRules(t *testing.T) {
	yes, no, empty := "yes", "no", ""
	cases := []struct {
		name  string
		a, b  Rule
		hosts []string
		valid bool
	}{
		{"different bodies", Rule{200, &yes}, Rule{200, &no}, nil, true},
		{"status only", Rule{204, nil}, Rule{401, nil}, nil, true},
		{"empty body is a rule", Rule{200, &empty}, Rule{200, &no}, nil, true},
		{"same bodies", Rule{200, &yes}, Rule{200, &yes}, nil, false},
		{"wildcard body overlaps", Rule{200, nil}, Rule{200, &no}, nil, false},
		{"missing success", Rule{}, Rule{401, nil}, nil, false},
		{"invalid status", Rule{600, nil}, Rule{401, nil}, nil, false},
		{"portal host", Rule{200, &yes}, Rule{401, nil}, []string{"login.example.edu"}, true},
		{"portal URL", Rule{200, &yes}, Rule{401, nil}, []string{"https://login.example.edu/"}, false},
		{"portal port", Rule{200, &yes}, Rule{401, nil}, []string{"login.example.edu:80"}, false},
		{"success redirect overlaps portal", Rule{302, nil}, Rule{401, nil}, []string{"login.example.edu"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := (Rules{Authenticated: tc.a, Unauthenticated: tc.b, PortalHosts: tc.hosts}).Validate()
			if (err == nil) != tc.valid {
				t.Fatalf("Validate()=%v; valid=%t", err, tc.valid)
			}
		})
	}
}

func TestLabelsAreIndependent(t *testing.T) {
	result := Result{Authenticated: AuthYes, ExternalAccess: "unknown"}
	first := result.Labels()
	first["network.campus/authenticated"] = "false"
	if result.Labels()["network.campus/authenticated"] != "true" {
		t.Fatal("label mutation changed result")
	}
}
