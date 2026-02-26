/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"maps"

	"github.com/google/go-github/github"
	syngit "github.com/syngit-org/syngit/pkg/api/v1beta4"
	syngitutils "github.com/syngit-org/syngit/pkg/utils"
	"golang.org/x/oauth2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlClient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// RemoteUserReconciler reconciles a RemoteUser object
type RemoteUserReconciler struct {
	ctrlClient.Client
	Scheme *runtime.Scheme
}

type RemoteUserChecker struct {
	remoteUser syngit.RemoteUser
	secret     corev1.Secret
}

// +kubebuilder:rbac:groups=syngit.io,resources=remoteusers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=syngit.io,resources=remoteusers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=syngit.io,resources=remoteusers/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch

func (r *RemoteUserReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)

	// Get the RemoteUser Object
	var remoteUser syngit.RemoteUser
	if err := r.Get(ctx, req.NamespacedName, &remoteUser); err != nil {
		// does not exists -> deleted
		return ctrl.Result{}, ctrlClient.IgnoreNotFound(err)
	}

	log.Log.Info("Reconcile request",
		"resource", "remoteuser",
		"namespace", remoteUser.Namespace,
		"name", remoteUser.Name,
	)

	remoteUserChecker := RemoteUserChecker{remoteUser: *remoteUser.DeepCopy()}

	var secret corev1.Secret
	namespacedNameSecret := types.NamespacedName{Namespace: req.Namespace, Name: remoteUser.Spec.SecretRef.Name}
	if err := r.Get(ctx, namespacedNameSecret, &secret); err != nil {
		remoteUserChecker.secret = corev1.Secret{}
	} else {
		remoteUserChecker.secret = secret
	}

	remoteUserChecker.testConnection()

	remoteUser.Status.Conditions = remoteUserChecker.remoteUser.Status.Conditions
	_ = r.updateStatus(ctx, req, remoteUserChecker.remoteUser.Status, 2)

	return ctrl.Result{}, nil
}

func (r *RemoteUserReconciler) updateStatus(ctx context.Context, req ctrl.Request, status syngit.RemoteUserStatus, retryNumber int) error {
	var remoteUser syngit.RemoteUser
	if err := r.Get(ctx, req.NamespacedName, &remoteUser); err != nil {
		return err
	}

	remoteUser.Status.ConnexionStatus = status.ConnexionStatus
	remoteUser.Status.Conditions = status.Conditions
	if err := r.Status().Update(ctx, &remoteUser); err != nil {
		if retryNumber > 0 {
			return r.updateStatus(ctx, req, status, retryNumber-1)
		}
		return err
	}
	return nil
}

func (ruc *RemoteUserChecker) testConnection() {
	conditions := ruc.remoteUser.Status.DeepCopy().Conditions

	if ruc.remoteUser.Annotations["github.syngit.io/auth.test"] != "true" {
		ruc.remoteUser.Status.Conditions = syngitutils.TypeBasedConditionRemover(conditions, "Authenticated")
	} else {
		if len(ruc.secret.Data) != 0 {
			ctx := context.Background()
			ts := oauth2.StaticTokenSource(
				&oauth2.Token{AccessToken: string(ruc.secret.Data["password"])},
			)
			tc := oauth2.NewClient(ctx, ts)

			client := github.NewClient(tc)
			user, _, err := client.Users.Get(ctx, "")
			if err != nil {
				condition := metav1.Condition{
					Type:               "Authenticated",
					Status:             metav1.ConditionFalse,
					Reason:             "AuthenticationFailed",
					Message:            err.Error(),
					LastTransitionTime: metav1.Now(),
				}
				ruc.remoteUser.Status.ConnexionStatus.Status = ""
				ruc.remoteUser.Status.ConnexionStatus.Details = err.Error()
				ruc.remoteUser.Status.Conditions = syngitutils.TypeBasedConditionUpdater(conditions, condition)
			} else {
				condition := metav1.Condition{
					Type:               "Authenticated",
					Status:             metav1.ConditionTrue,
					Reason:             "AuthenticationSucceded",
					Message:            fmt.Sprintf("Authentication was successful with the user %s", user.GetLogin()),
					LastTransitionTime: metav1.Now(),
				}
				ruc.remoteUser.Status.ConnexionStatus.Details = ""
				ruc.remoteUser.Status.ConnexionStatus.Status = syngit.GitAuthenticated
				ruc.remoteUser.Status.Conditions = syngitutils.TypeBasedConditionUpdater(conditions, condition)
			}
		}
	}
}

func (r *RemoteUserReconciler) findObjectsForSecret(ctx context.Context, secret ctrlClient.Object) []reconcile.Request {
	attachedRemoteUsers := &syngit.RemoteUserList{}
	listOps := &ctrlClient.ListOptions{
		FieldSelector: fields.OneTermEqualSelector(syngit.SecretRefField, secret.GetName()),
		Namespace:     secret.GetNamespace(),
	}
	err := r.List(ctx, attachedRemoteUsers, listOps)
	if err != nil {
		return []reconcile.Request{}
	}

	requests := make([]reconcile.Request, len(attachedRemoteUsers.Items))
	for i, item := range attachedRemoteUsers.Items {
		requests[i] = reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      item.GetName(),
				Namespace: item.GetNamespace(),
			},
		}
	}
	return requests
}

// SetupWithManager sets up the controller with the Manager.
func (r *RemoteUserReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &syngit.RemoteUser{}, syngit.SecretRefField, func(rawObj ctrlClient.Object) []string {
		// Extract the Secret name from the RemoteUser Spec, if one is provided
		remoteUser := rawObj.(*syngit.RemoteUser)
		if remoteUser.Spec.SecretRef.Name == "" {
			return nil
		}
		return []string{remoteUser.Spec.SecretRef.Name}
	}); err != nil {
		return err
	}

	p := predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldObject, _ := e.ObjectOld.(*syngit.RemoteUser)
			newObject, _ := e.ObjectNew.(*syngit.RemoteUser)

			if newObject != nil {
				if !maps.Equal(oldObject.DeepCopy().Labels, newObject.DeepCopy().Labels) {
					return true
				}
				if !maps.Equal(oldObject.DeepCopy().Annotations, newObject.DeepCopy().Annotations) {
					return true
				}
				if oldObject.DeepCopy().Spec != newObject.Spec {
					return true
				}
			}
			return false
		},
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&syngit.RemoteUser{}, builder.WithPredicates(p)).
		Named("remoteuser").
		Watches(
			&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.findObjectsForSecret),
			builder.WithPredicates(predicate.ResourceVersionChangedPredicate{}),
		).
		Complete(r)
}
